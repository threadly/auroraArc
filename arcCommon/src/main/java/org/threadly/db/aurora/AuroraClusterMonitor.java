package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
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
 * there is exactly one master server, and zero or more replica servers which can be used for
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
          acm.clusterChecker.updateServerCheckDelayMillis(millis);
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
      errorExceptionHandler = (server, error) -> { /* ignored */ };
    } else {
      errorExceptionHandler = handler;
    }
  }
  
  /**
   * Set a {@link AuroraClusterStateListener} to be invoked to provide status updates on cluster 
   * state changes.  See the interface definition for details about each action.  It's critical 
   * that no invocation of this listener blocks as it could block the cluster check thread.
   * <p>
   * After being set, on the next cluster check the listener will be invoked with 
   * {@link AuroraClusterStateListener#newWriter(AuroraServer)} and 
   * {@link AuroraClusterStateListener#newReplica(AuroraServer)} as appropriate to initialize the 
   * current cluster state.
   * <p>
   * If the cluster is unable to be looked up an error will be thrown, thus it is important that 
   * the database has been initialized before invoking this.
   * 
   * @param Collection of the aurora hosts which describe the cluster, should include `:port` suffixes
   * @param listener Listener to be invoked on cluster state checks
   */
  public static void setClusterStateListener(Collection<String> clusterHosts, 
                                             AuroraClusterStateListener listener) {
    AuroraServer[] aServers = new AuroraServer[clusterHosts.size()];
    int index = 0;
    for (String host : clusterHosts) {
      aServers[index++] = new AuroraServer(host, null);
    }
    
    AuroraClusterMonitor clusterMonitor = MONITORS.get(new AuroraServersKey(aServers));
    if (clusterMonitor == null) {
      throw new RuntimeException("Could not find cluster, make sure it's running: " + clusterHosts);
    }
    clusterMonitor.clusterChecker.setClusterStateListener(listener);
  }
  
  /**
   * Invoke to set the weight for a given replica.  This only impacts when a random replica is 
   * requested, and wont impact the master server usage.  The weight must be between {@code 0} and 
   * {@code 100} inclusive.  Ever server starts with a default weight of {@code 1}.  A weight of 
   * {@code 0} indicates to not use the replica unless no other healthy replicas are available.  
   * When a weight higher than the default of {@code 1} is used, it is in effect as if that server 
   * was in the cluster multiple times.  For example a cluster with a server X of weight 1, and 
   * server Y of weight 2, would send 2/3's of the requests to server Y.
   * 
   * @param host The host to update against
   * @param port The port for the replica server
   * @param weight The weight to apply to this replica
   */
  public static void setReplicaWeight(String host, int port, int weight) {
    if (weight < 0) {
      throw new IllegalArgumentException("Negative server weights not allowed");
    } else if (weight > 100) {
      throw new IllegalArgumentException("Maximum allowed weight is 100");
    } else if (MONITORS.isEmpty()) {
      throw new IllegalStateException("No aurora clusters monitored, make sure database is setup");
    }
    
    boolean updated = false;
    for (AuroraClusterMonitor acm : MONITORS.values()) {
      updated |= acm.clusterChecker.queueReplicaWeightUpdate(host, port, weight);
    }
    if (! updated) {
      throw new IllegalStateException("Could not find server: " + host + ":" + port);
    }
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
  
  /**
   * Listener invoked on Aurora cluster state changes.  It is critical that not invocations block 
   * as it could impact monitoring the health of the cluster.
   * 
   * @since 0.14
   */
  public interface AuroraClusterStateListener {
    /**
     * Listener which ignores all invocations.
     */
    public AuroraClusterStateListener IGNORE_LISTENER = new AuroraClusterStateListener() {
      @Override
      public void newWriter(AuroraServer newMaster) { /* ignored */ }

      @Override
      public void unhealthyWriter(AuroraServer formerMaster) { /* ignored */ }

      @Override
      public void newReplica(AuroraServer newReplica) { /* ignored */ }

      @Override
      public void unhealthyReplica(AuroraServer formerReplica) { /* ignored */ }

      @Override
      public void clusterCheckComplete() { /* ignored */ }
    };
    
    /**
     * Invoked when a new server has become a writer.
     * 
     * @param newMaster Server which is now the cluster master
     */
    public void newWriter(AuroraServer newMaster);
    
    /**
     * Invoked when a former master (notified through {@link #newWriter(AuroraServer)}) has become 
     * unhealthy.
     * 
     * @param formerMaster Server which is no longer a writer
     */
    public void unhealthyWriter(AuroraServer formerMaster);
    
    /**
     * Invoked when a replica server has joined the cluster.
     * 
     * @param newReplica Server which can be used as a replica
     */
    public void newReplica(AuroraServer newReplica);

    /**
     * Invoked when a former replica (notified through {@link #newReplica(AuroraServer)} has become 
     * unhealthy.
     * 
     * @param formerReplica Server which is no longer a replica
     */
    public void unhealthyReplica(AuroraServer formerReplica);

    /**
     * Invoked at the completion of every cluster check.  This can be used to monitor the cluster 
     * checker health, as well as commit pending cluster changes from other calls.
     */
    public void clusterCheckComplete();
  }

  protected final ClusterChecker clusterChecker;
  private final AtomicLong replicaIndex;  // used to distribute replica reads

  protected AuroraClusterMonitor(SchedulerService scheduler, long checkIntervalMillis,
                                 DelegateAuroraDriver driver, AuroraServer[] clusterServers) {
    this(new ClusterChecker(scheduler, checkIntervalMillis, driver, clusterServers));
  }

  // used in testing
  protected AuroraClusterMonitor(ClusterChecker clusterChecker) {
    this.clusterChecker = clusterChecker;
    replicaIndex = new AtomicLong();
  }
  
  /**
   * Getter for the current number of healthy replicate servers known in the cluster.  The master 
   * server is not included in this count.
   * 
   * @return The number of healthy replica servers in the cluster
   */
  public int getHealthyReplicaCount() {
    return clusterChecker.replicaServers.size();
  }

  /**
   * Return a random read only replica server.
   *
   * @return Random read only replica server, or {@code null} if no servers are healthy
   */
  public AuroraServer getRandomReadReplica() {
    while (true) {
      int replicaCount = clusterChecker.replicaServers.size();
      try {
        if (replicaCount == 1) {
          return clusterChecker.replicaServers.get(0);
        } else if (replicaCount == 0) {
          return null;
        } else if (clusterChecker.weightedReplicaServers.isEmpty()) {
          // no weighted replica's, meaning all healthy replica's have a weight of 0, pick random zero weight
          return clusterChecker.replicaServers
                     .get((int)(replicaIndex.getAndIncrement() % replicaCount));
        } else {
          return clusterChecker.weightedReplicaServers
                     .get((int)(replicaIndex.getAndIncrement() % clusterChecker.weightedReplicaServers.size()));
        }
      } catch (IndexOutOfBoundsException e) {
        // replica server was removed during check, loop and retry
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
    if (clusterChecker.replicaServers.size() <= index) {
      return null;
    }
    try {
      return clusterChecker.replicaServers.get(index);
    } catch (IndexOutOfBoundsException e) {
      // replica server was removed after check
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
    return clusterChecker.masterServer.get();
  }

  /**
   * Indicate a given server should be expedited in checking its current state.  If a given
   * connection finds issues with a suggested server informing the monitor of the issues can ensure
   * that everyone using this cluster can be informed of the state quicker.
   *
   * @param auroraServer Server identifier to be checked
   */
  public void expediteServerCheck(AuroraServer auroraServer) {
    clusterChecker.expediteServerCheck(auroraServer);
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
    protected final List<AuroraServer> replicaServers;
    protected volatile List<AuroraServer> weightedReplicaServers; // reference replaced when updated
    protected final AtomicReference<AuroraServer> masterServer;
    protected final CopyOnWriteArrayList<AuroraServer> serversWaitingExpeditiedCheck;
    protected final ConcurrentMap<AuroraServer, Integer> pendingServerWeightUpdates;
    private AuroraClusterStateListener clusterStateListener;
    private volatile AuroraClusterStateListener replacementClusterStateListener = null;
    private volatile boolean initialized = false; // starts false to avoid updates while constructor is running

    protected ClusterChecker(SchedulerService scheduler, long checkIntervalMillis,
                             DelegateAuroraDriver driver, AuroraServer[] clusterServers) {
      super(scheduler, 0);

      this.scheduler = scheduler;
      allServers = new HashMap<>();
      replicaServers = new CopyOnWriteArrayList<>();
      weightedReplicaServers = new ArrayList<>(clusterServers.length);
      masterServer = new AtomicReference<>();
      serversWaitingExpeditiedCheck = new CopyOnWriteArrayList<>();
      pendingServerWeightUpdates = new ConcurrentHashMap<>();
      clusterStateListener = AuroraClusterStateListener.IGNORE_LISTENER;

      for (AuroraServer server : clusterServers) {
        ServerMonitor monitor = new ServerMonitor(scheduler, driver, server, this);
        allServers.put(server, monitor);

        if (masterServer.get() == null) {  // check in thread till we find the master
          monitor.run(true);
          if (monitor.isHealthy()) {
            if (monitor.isMasterServer()) {
              masterServer.set(server);
            } else {
              replicaServers.add(server);
              
              addWeightedServer(weightedReplicaServers, server);
            }
          }
        } else {  // all other checks can be async, adding in as replica servers as they complete
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
    
    public void setClusterStateListener(AuroraClusterStateListener listener) {
      // set replacementClusterStateListener so that listener can be updated only on the cluster check thread
      if (listener == null) {
        replacementClusterStateListener = AuroraClusterStateListener.IGNORE_LISTENER;
      } else {
        replacementClusterStateListener = listener;
      }
      signalToRun();
    }

    /**
     * Invoke to queue a replica weight update the next time the server state is checked.
     * 
     * @param host The host to update against
     * @param port The port for the replica server
     * @param weight The weight to apply to this replica
     * @return {@code true} if a server was identified to queue the weight update against
     */
    public boolean queueReplicaWeightUpdate(String host, int port, int weight) {
      for (AuroraServer as : allServers.keySet()) {
        if (as.matchHost(host, port)) {
          // replica weight is queued so that the cluster checking thread can be the only thread 
          // which modifies both the weightedReplicaServers collection and the weight in AuroraServer
          pendingServerWeightUpdates.put(as, weight);
          signalToRunImmediately(false);
          return true;
        }
      }
      return false;
    }

    // used in testing
    protected ClusterChecker(SchedulerService scheduler, 
                             Map<AuroraServer, ServerMonitor> clusterServers) {
      super(scheduler, 0);

      this.scheduler = scheduler;
      allServers = clusterServers;
      replicaServers = new CopyOnWriteArrayList<>();
      weightedReplicaServers = Collections.emptyList();
      masterServer = new AtomicReference<>();
      serversWaitingExpeditiedCheck = new CopyOnWriteArrayList<>();
      pendingServerWeightUpdates = new ConcurrentHashMap<>();
      clusterStateListener = AuroraClusterStateListener.IGNORE_LISTENER;

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
    
    private static void addWeightedServer(List<AuroraServer> weightedReplicaServers, AuroraServer as) {
      int weightedCount = weightedReplicaServers.size();
      ThreadLocalRandom random = null;
      for (int i = 0; i < as.getWeight(); i++) {
        // add one for each weight, choose an index that distributes the servers in the list
        int index;
        if (i == 0) { // first one goes at end for efficiency
          index = weightedCount;
        } else if (i == 1) { // place second one at farthest distance from first
          index = weightedCount / 2;
        } else { // after that just place randomly in the list
          if (random == null) {
            random = ThreadLocalRandom.current();
          }
          index = random.nextInt(weightedCount);
        }
        weightedReplicaServers.add(index, as);
        weightedCount++;
      }
    }
    
    private static void removeWeightedServer(List<AuroraServer> weightedReplicaServers, AuroraServer as) {
      weightedReplicaServers.removeIf(as::equals);
    }
    
    private ArrayList<AuroraServer> copyWeightedReplicaServers() {
      ArrayList<AuroraServer> weightedReplicaServersCopy = 
          new ArrayList<>(64);  // sized to be larger than likely needed, but still cheap to allocate
      weightedReplicaServersCopy.addAll(weightedReplicaServers);
      return weightedReplicaServersCopy;
    }

    @Override
    protected void run() {
      if (! initialized) {
        return; // ignore state updates still initialization is done
      }
      
      if (replacementClusterStateListener != null) {
        clusterStateListener = replacementClusterStateListener;
        replacementClusterStateListener = null;
        
        // invoke with the current cluster state
        AuroraServer as = masterServer.get();
        if (as != null) {
          clusterStateListener.newWriter(as);
        }
        replicaServers.forEach(clusterStateListener::newReplica);
      }
      
      ArrayList<AuroraServer> updatedWeightedReplicaServers = null;
      if (! pendingServerWeightUpdates.isEmpty()) {
        Iterator<Map.Entry<AuroraServer, Integer>> it = pendingServerWeightUpdates.entrySet().iterator();
        do {
          Map.Entry<AuroraServer, Integer> entry = it.next();
          it.remove();
          AuroraServer as = entry.getKey();
          int weight = entry.getValue();
          
          if (weight == as.getWeight()) {
            continue;
          }
          
          as.setWeight(weight); // set weight in AuroraServer instance
          
          if (replicaServers.contains(as)) {
            // update weighted list if server may be used
            if (updatedWeightedReplicaServers == null) {
              updatedWeightedReplicaServers = copyWeightedReplicaServers();
            }
            removeWeightedServer(updatedWeightedReplicaServers, as);
            addWeightedServer(updatedWeightedReplicaServers, as);
          }
        } while (it.hasNext());
      }
      
      for (Map.Entry<AuroraServer, ServerMonitor> entry : allServers.entrySet()) {
        AuroraServer as = entry.getKey();
        ServerMonitor monitor = entry.getValue();
        
        if (monitor.isHealthy()) {
          if (monitor.isMasterServer()) {
            if (! as.equals(masterServer.get())) {
              // either already was the master, or now is
              masterServer.set(as);
              
              clusterStateListener.newWriter(as);
              LOG.info("New master server: " + as);
            }
          } else {
            if (as.equals(masterServer.get())) {
              // no longer a master, will need to find the new one
              masterServer.compareAndSet(as, null);
              checkAllServers();  // make sure we find a new master ASAP
              
              clusterStateListener.unhealthyWriter(as);
              clusterStateListener.newReplica(as);
            }
            
            if (! replicaServers.contains(as)) {
              replicaServers.add(as);

              if (as.getWeight() > 0) {
                if (updatedWeightedReplicaServers == null) {
                  updatedWeightedReplicaServers = copyWeightedReplicaServers();
                }
                addWeightedServer(updatedWeightedReplicaServers, as);
              }
              
              clusterStateListener.newReplica(as);
            }
          }
        } else {
          if (replicaServers.remove(as)) {
            if (as.getWeight() > 0) {
              if (updatedWeightedReplicaServers == null) {
                updatedWeightedReplicaServers = copyWeightedReplicaServers();
              }
              removeWeightedServer(updatedWeightedReplicaServers, as);
            }
            
            clusterStateListener.unhealthyReplica(as);
            // TODO - removal of a replica server can indicate it will become a new primary
            //        How can we use this common behavior to recover quicker?
          } else if (as.equals(masterServer.get())) {
            // no master till we find a new one
            masterServer.compareAndSet(as, null);
            checkAllServers();  // make sure we find a new master ASAP
            
            clusterStateListener.unhealthyWriter(as);
          } // else, server is already known to be unhealthy, no change
        }
      }
      
      if (updatedWeightedReplicaServers != null) {
        if (updatedWeightedReplicaServers.isEmpty()) {
          weightedReplicaServers = Collections.emptyList();
        } else if (updatedWeightedReplicaServers.size() == 1) {
          weightedReplicaServers = Collections.singletonList(updatedWeightedReplicaServers.get(0));
        } else {
          updatedWeightedReplicaServers.trimToSize();
          weightedReplicaServers = updatedWeightedReplicaServers;
        }
      }
      
      clusterStateListener.clusterCheckComplete();
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
