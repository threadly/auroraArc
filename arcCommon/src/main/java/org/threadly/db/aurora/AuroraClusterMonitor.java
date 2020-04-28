package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.db.ErrorInvalidSqlConnection;
import org.threadly.db.aurora.DelegateAuroraDriver.IllegalDriverStateException;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionHandler;

/**
 * Class which monitors a "cluster" of aurora servers.  It is expected that for each given cluster
 * there is exactly one master server, and zero or more secondary servers which can be used for
 * reads.  This class will monitor those clusters, providing a monitor through
 * {@link AuroraClusterMonitor#getMonitor(DelegateAuroraDriver, AuroraServer[])}.
 * <p>
 * Currently there is no way to stop monitoring a cluster.  So changes (both additions and removals)
 * of aurora clusters will currently need a JVM restart in order to have the new monitoring behave
 * correctly.
 */
public class AuroraClusterMonitor {
  protected static final Logger LOG = Logger.getLogger(AuroraClusterMonitor.class.getSimpleName());
  protected static final int CONNECTION_VALID_CHECK_TIMEOUT = 10_000;

  protected static final int MAXIMUM_THREAD_POOL_SIZE = 64;
  protected static final SchedulerService MONITOR_SCHEDULER;
  protected static final ConcurrentMap<AuroraServersKey, AuroraClusterMonitor> MONITORS;
  private static volatile long checkFrequencyMillis = 500;
  private static volatile BiConsumer<AuroraServer, Throwable> errorExceptionHandler;

  static {
    MONITOR_SCHEDULER = CentralThreadlyPool.threadPool(MAXIMUM_THREAD_POOL_SIZE, "auroraMonitor");

    MONITORS = new ConcurrentHashMap<>();
    setExceptionHandler(null);
  }
  
  /**
   * Sets or updates the delay between individual server status checks.  Reducing this from the 
   * default of 500ms can make failover events be discovered faster.  Since this is done on a 
   * per-client basis, it is recommended not to make this too small or it can significantly impact 
   * server load when there is a lot of clients.  It is worth being aware that server checks will 
   * be expedited if the driver discovers potential server stability issues anyways.
   * 
   * @param millis The milliseconds between server checks
   */
  public static void setServerCheckDelayMillis(long millis) {
    ArgumentVerifier.assertGreaterThanZero(millis, "millis");
    
    synchronized (AuroraClusterMonitor.class) {
      if (checkFrequencyMillis != millis) {
        checkFrequencyMillis = millis;
        
        for (AuroraClusterMonitor acm : MONITORS.values()) {
          acm.clusterStateChecker.updateServerCheckDelayMillis(millis);
        }
      }
    }
  }
  
  /**
   * Set an {@link ExceptionHandler} to be invoked on EVERY error when checking the cluster state.
   * By default state changes will be logged, but due to potential high volumes logs will only 
   * occur on server state changes.  This sets an {@link ExceptionHandler} which will be invoked on 
   * any error, even if the server is already in an unhealthy condition.
   * 
   * @param handler Handler to be invoked or {@code null} if exceptions should be dropped.
   */
  public static void setExceptionHandler(BiConsumer<AuroraServer, Throwable> handler) {
    if (handler == null) {
      handler = (server, error) -> { /* ignored */ };
    }
    errorExceptionHandler = handler;
  }

  /**
   * Return a monitor instance for a given set of servers.  This instance will be consistent as
   * long as an equivalent servers set is provided.
   *
   * @param driver DelegateDriver to use for connecting to cluster members if needed
   * @param servers Set of servers within the cluster, expected to be de-duplicated, but order does not matter
   * @return Monitor to select healthy and well balanced cluster servers
   */
  protected static AuroraClusterMonitor getMonitor(DelegateAuroraDriver driver, AuroraServer[] servers) {
    // the implementation of `AbstractSet`'s equals will verify the size, followed by `containsAll`.
    // Since order and other variations should not impact the lookup we just store the provided set 
    // into the map directly for efficiency when the same set is provided multiple times
    
    AuroraServersKey mapKey = new AuroraServersKey(servers);
    AuroraClusterMonitor result = MONITORS.get(mapKey);
    if (result != null) {
      return result;
    }
    
    return MONITORS.computeIfAbsent(mapKey,
                                    (s) -> new AuroraClusterMonitor(MONITOR_SCHEDULER,
                                                                    checkFrequencyMillis, 
                                                                    driver, mapKey.clusterServers));
  }

  protected final ClusterChecker clusterStateChecker;
  private final AtomicLong replicaIndex;  // used to distribute replica reads

  protected AuroraClusterMonitor(SchedulerService scheduler, long checkIntervalMillis,
                                 DelegateAuroraDriver driver, AuroraServer[] clusterServers) {
    this(new ClusterChecker(scheduler, checkIntervalMillis, driver, clusterServers));
  }

  // used in testing
  protected AuroraClusterMonitor(ClusterChecker clusterChecker) {
    this.clusterStateChecker = clusterChecker;
    replicaIndex = new AtomicLong();
  }
  
  /**
   * Getter for the current number of healthy replicate servers known in the cluster.  The master 
   * server is not included in this count.
   * 
   * @return The number of healthy replica servers in the cluster
   */
  public int getHealthyReplicaCount() {
    return clusterStateChecker.secondaryServers.size();
  }

  /**
   * Return a random read only replica server.
   *
   * @return Random read only replica server, or {@code null} if no servers are healthy
   */
  public AuroraServer getRandomReadReplica() {
    return getRandomFrom(clusterStateChecker.secondaryServers);
  }

  /**
   * Return a random colocated server.
   *
   * @return Random colocated server, or {@code null} if there are no healthy colocated servers
   */
  public AuroraServer getRandomColocatedServer() {
    return getRandomFrom(clusterStateChecker.colocatedServers);
  }


  private AuroraServer getRandomFrom(List<AuroraServer> servers) {
    while (true) {
      int count = servers.size();
      try {
        if (count == 1) {
          return servers.get(0);
        } else if (count == 0) {
          return null;
        } else {
          long replicaIndex = this.replicaIndex.getAndIncrement();
          return servers.get((int)(replicaIndex & 0x7FFF_FFFF) % count);
        }
      } catch (IndexOutOfBoundsException e) {
        // secondary server was removed during check, loop and retry
      }
    }
  }
  
  /**
   * Return a read replica which corresponds to an index.  A given index may not always provide the 
   * same server depending on membership changes or other re-ordering that may occur.  But should 
   * provide a basic way to get varying servers when the internal randomization mechanism is not 
   * desired.
   * 
   * @param index Zero based index of replica to provide
   * @return Read only replica server, or {@code null} if no servers was at the provided index
   */
  public AuroraServer getReadReplica(int index) {
    if (clusterStateChecker.secondaryServers.size() <= index) {
      return null;
    }
    try {
      return clusterStateChecker.secondaryServers.get(index);
    } catch (IndexOutOfBoundsException e) {
      // secondary server was removed after check
      return null;
    }
  }

  /**
   * Returns the current cluster master which can accept writes.  May be {@code null} if there is
   * not currently any healthy cluster members which can accept writes.
   *
   * @return Current writable server or {@code null} if no known one exists
   */
  public AuroraServer getCurrentMaster() {
    return clusterStateChecker.masterServer.get();
  }

  /**
   * Indicate a given server should be expedited in checking its current state.  If a given
   * connection finds issues with a suggested server informing the monitor of the issues can ensure
   * that everyone using this cluster can be informed of the state quicker.
   *
   * @param auroraServer Server identifier to be checked
   */
  public void expediteServerCheck(AuroraServer auroraServer) {
    clusterStateChecker.expediteServerCheck(auroraServer);
  }
  
  /**
   * Class to provide efficient checks against an array of servers.  This used to be referenced as 
   * {@code Set<AuroraServer>} but that would result in an iterator being created and iterated 
   * twice as the hashCode and equality is checked.
   * 
   * @since 0.8
   */
  protected static class AuroraServersKey {
    private final AuroraServer[] clusterServers;
    private final int hashCode;
    
    public AuroraServersKey(AuroraServer[] clusterServers) {
      this.clusterServers = clusterServers;
      int h = 0;
      for (int i = 0; i < clusterServers.length; i++) {
        // use addition so order wont matter
        h += clusterServers[i].hashCode();
      }
      this.hashCode = h;
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      try {
        AuroraServersKey sk = (AuroraServersKey)o;
        if (clusterServers.length != sk.clusterServers.length) {
          return false;
        } else if (hashCode != sk.hashCode) {
          return false;
        }
        containsAllLoop: for (int i1 = 0; i1 < clusterServers.length; i1++) {
          if (clusterServers[i1].equals(sk.clusterServers[i1])) {
            continue containsAllLoop;
          }
          for (int i2 = 0; i2 < clusterServers.length; i2++) {
            if (i2 != i1 && clusterServers[i1].equals(sk.clusterServers[i2])) {
              continue containsAllLoop;
            }
          }
          return false;
        }
        return true;
      } catch (ClassCastException e) {
        return false;
      }
    }
  }

  /**
   * Checks across the state of all servers as they change to see which are potential primaries and
   * which are potential read only slaves.  The use of {@link ReschedulingOperation} with a delay
   * of {@code 0} allows us to ensure this runs as soon as possible when state changes, not run
   * concurrently, and also ensure that we always check the state after modifications have finished
   * (and thus will witness the final cluster state).
   */
  protected static class ClusterChecker extends ReschedulingOperation {
    protected final SchedulerService scheduler;
    protected final Map<AuroraServer, ServerMonitor> allServers;
    protected final List<AuroraServer> secondaryServers;
    protected final List<AuroraServer> colocatedServers;
    protected final AtomicReference<AuroraServer> masterServer;
    protected final CopyOnWriteArrayList<AuroraServer> serversWaitingExpeditiedCheck;
    private volatile boolean initialized = false; // starts false to avoid updates while constructor is running

    protected ClusterChecker(SchedulerService scheduler, long checkIntervalMillis,
                             DelegateAuroraDriver driver, AuroraServer[] clusterServers) {
      super(scheduler, 0);

      this.scheduler = scheduler;
      allServers = new HashMap<>();
      secondaryServers = new CopyOnWriteArrayList<>();
      colocatedServers = new CopyOnWriteArrayList<>();
      serversWaitingExpeditiedCheck = new CopyOnWriteArrayList<>();
      masterServer = new AtomicReference<>();

      for (AuroraServer server : clusterServers) {
        ServerMonitor monitor = new ServerMonitor(scheduler, driver, server, this);
        allServers.put(server, monitor);

        if (masterServer.get() == null) {  // check in thread till we find the master
          monitor.run(true);
          if (monitor.isHealthy()) {
            if (monitor.isMasterServer()) {
              masterServer.set(server);
            } else {
              secondaryServers.add(server);
            }

            if (server.isColocated()) {
              colocatedServers.add(server);
            }
          }
        } else {  // all other checks can be async, adding in as secondary servers as they complete
          scheduler.execute(monitor);
        }

        scheduleMonitor(monitor, checkIntervalMillis);
      }
      if (masterServer.get() == null) {
        LOG.warning("No master server found!  Will use read only servers till one becomes master");
      }
      
      initialized = true;
      signalToRun();
    }

    // used in testing
    protected ClusterChecker(SchedulerService scheduler, 
                             Map<AuroraServer, ServerMonitor> clusterServers) {
      super(scheduler, 0);

      this.scheduler = scheduler;
      allServers = clusterServers;
      secondaryServers = new CopyOnWriteArrayList<>();
      colocatedServers = new CopyOnWriteArrayList<>();
      serversWaitingExpeditiedCheck = new CopyOnWriteArrayList<>();
      masterServer = new AtomicReference<>();

      initialized = true;
    }

    
    /**
     * Update how often monitors check the individual servers status.
     * <p>
     * This can NOT be called concurrently, 
     * {@link AuroraClusterMonitor#setClusterCheckFrequencyMillis(long)} currently guards this.
     * 
     * @param millis The delay between runs for monitoring server status  
     */
    protected void updateServerCheckDelayMillis(long millis) {
      for (ServerMonitor sm : allServers.values()) {
        if (scheduler.remove(sm)) {
          scheduleMonitor(sm, millis);
        } else {
          throw new IllegalStateException("Could not unschedule monitor: " + sm);
        }
      }
    }
    
    protected void scheduleMonitor(ServerMonitor monitor, long checkIntervalMillis) {
      scheduler.scheduleAtFixedRate(monitor,
                                    // hopefully well distributed hash code will distribute
                                    // these tasks so that they are not all checked at once
                                    // we convert to a long to avoid a possible overflow at Integer.MIN_VALUE
                                    Math.abs((long)System.identityHashCode(monitor)) % checkIntervalMillis,
                                    checkIntervalMillis);
    }

    protected void expediteServerCheck(ServerMonitor serverMonitor) {
      if (serversWaitingExpeditiedCheck.addIfAbsent(serverMonitor.server)) {
        scheduler.execute(() -> {
          try {
            serverMonitor.run(true);
          } finally {
            serversWaitingExpeditiedCheck.remove(serverMonitor.server);
          }
        });
      }
    }

    public void expediteServerCheck(AuroraServer auroraServer) {
      ServerMonitor monitor = allServers.get(auroraServer);
      if (monitor != null) {
        expediteServerCheck(monitor);
      }
    }

    protected void checkAllServers() {
      for (ServerMonitor sm : allServers.values()) {
        expediteServerCheck(sm);
      }
    }

    @Override
    protected void run() {
      if (! initialized) {
        // ignore state updates still initialization is done
        return;
      }
      
      for (Map.Entry<AuroraServer, ServerMonitor> p : allServers.entrySet()) {
        if (p.getValue().isHealthy()) {
          if (p.getValue().isMasterServer()) {
            if (! p.getKey().equals(masterServer.get())) {
              // either already was the master, or now is
              masterServer.set(p.getKey());
              LOG.info("New master server: " + p.getKey());
            }
          } else {
            if (p.getKey().equals(masterServer.get())) {
              // no longer a master, will need to find the new one
              masterServer.compareAndSet(p.getKey(), null);
              checkAllServers();  // make sure we find a new master ASAP
            }
            if (! secondaryServers.contains(p.getKey())) {
              secondaryServers.add(p.getKey());
            }
          }

          if (p.getKey().isColocated() && !colocatedServers.contains(p.getKey())) {
            colocatedServers.add(p.getKey());
          }
        } else {
          if (p.getKey().isColocated()) {
            colocatedServers.remove(p.getKey());
          }

          if (secondaryServers.remove(p.getKey())) {
            // was secondary, so done
            // TODO - removal of a secondary server can indicate it will become a new primary
            //        How can we use this common behavior to recover quicker?
          } else if (p.getKey().equals(masterServer.get())) {
            // no master till we find a new one
            masterServer.compareAndSet(p.getKey(), null);
            checkAllServers();  // make sure we find a new master ASAP
          } // else, server is already known to be unhealthy, no change
        }
      }
    }
  }

  /**
   * Class which when executed checks against a remote aurora server to see if it is the master,
   * and as a form of a basic health check.  This status from the last execution can then be
   * queried.
   */
  protected static class ServerMonitor implements Runnable {
    protected final SubmitterScheduler scheduler;
    protected final DelegateAuroraDriver driver;
    protected final AuroraServer server;
    protected final ReschedulingOperation clusterStateChecker;
    protected final AtomicBoolean running;
    protected Connection serverConnection;  // guarded by running AtomicBoolean
    protected Throwable lastError;  // guarded by running AtomicBoolean
    protected volatile Throwable stateError;
    protected volatile boolean masterServer;

    protected ServerMonitor(SubmitterScheduler scheduler, DelegateAuroraDriver driver, 
                            AuroraServer server, ReschedulingOperation clusterStateChecker) {
      this.scheduler = scheduler;
      this.driver = driver;
      this.server = server;
      this.clusterStateChecker = clusterStateChecker;
      this.running = new AtomicBoolean(false);
      reconnect();
      lastError = null;
      masterServer = false;
    }
    
    @Override
    public String toString() {
      return (masterServer ? "m:" : "r:") + server;
    }
    
    protected void reconnect() {
      Connection newConnection;
      try {
        newConnection = driver.connect(server.hostAndPortString() +
                                         "/?connectTimeout=10000&socketTimeout=10000" + 
                                         driver.getStatusConnectURLParams(),
                                       server.getProperties());
      } catch (SQLException e) {
        newConnection = new ErrorInvalidSqlConnection(null, e);
      }
      if (serverConnection != null) { // only attempt to replace once we have a new connection without exception
        try {
          serverConnection.close();
        } catch (SQLException e) {
          // ignore errors closing connection
        }
      }
      serverConnection = newConnection;
    }

    public boolean isHealthy() {
      return stateError == null;
    }

    public boolean isMasterServer() {
      return masterServer;
    }

    @Override
    public void run() {
      run(false);
    }
    
    public void run(boolean expedited) {
      // if already running ignore start
      if (running.compareAndSet(false, true)) {
        try {
          updateState(expedited);
        } finally {
          running.set(false);
        }
      }
    }

    protected void updateState(boolean expedited) {
      // we can't set our class state directly, as we need to indicate if it has changed
      boolean currentlyMasterServer = false;
      Throwable currentError = null;
      try {
        if (! serverConnection.isValid(CONNECTION_VALID_CHECK_TIMEOUT)) {
          // try to avoid an error that will mark as unhealthy, if invalid because exception was 
          // thrown during reconnect the failure will be thrown when the connection is to be used
          reconnect();
        }
        currentlyMasterServer = driver.isMasterServer(server, serverConnection);
      } catch (IllegalDriverStateException e) {
        // these exceptions are handled different
        LOG.severe(e.getMessage());
        currentlyMasterServer = false;
      } catch (Throwable t) {
        currentError = t;
      } finally {
        boolean updateClusterState = false;
        if (currentlyMasterServer != masterServer) {
          masterServer = currentlyMasterServer;
          updateClusterState = true;
        }
        // NO else if, do the above checks in addition
        if (currentError == null) {
          if (lastError != null) {
            LOG.log(Level.INFO, "Aurora server " + server + " is healthy again");
            lastError = null;
            if (stateError != null) {
              stateError = null;
              updateClusterState = true;
            }
          }
        } else if (lastError == null && ! expedited) {
          LOG.log(Level.WARNING,
                  "Scheduling aurora server " + server + " recheck after first error", currentError);
          lastError = currentError;
          // don't update state status yet, wait till next failure, but schedule a recheck soon
          // schedule as a lambda so it wont be confused with potential .remove(Runnable) invocations
          // TODO - we may not always want to expedite this check, ie if existing connections can work
          scheduler.schedule(() -> run(true), checkFrequencyMillis / 2);
        } else {
          lastError = stateError = currentError;
          if (stateError == null) {
            LOG.log(Level.WARNING,
                    "Setting aurora server " + server + " as unhealthy due to error", currentError);
            updateClusterState = true;
          }
        }
        
        if (updateClusterState) {
          clusterStateChecker.signalToRun();
        }
        // reconnect after state has been reflected, don't block till it is for sure known we are unhealthy
        if (currentError != null) {
          reconnect();
          errorExceptionHandler.accept(server, currentError);
        }
      }
    }
  }
}
