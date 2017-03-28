package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.TaskPriority;

/**
 * Class which monitors a "cluster" of aurora servers.  It is expected that for each given cluster
 * there is exactly one master server, and zero or more secondary servers which can be used for
 * reads.  This class will monitor those clusters, providing a monitor through
 * {@link AuroraClusterMonitor#getMonitor(Set)}.
 * <p>
 * Currently there is no way to stop monitoring a cluster.  So changes (both additions and removals)
 * of aurora clusters will currently need a JVM restart in order to have the new monitoring behave
 * correctly.
 */
public class AuroraClusterMonitor {
  private static final Logger log = LoggerFactory.getLogger(AuroraClusterMonitor.class);

  private static final int CHECK_FREQUENCY_MILLIS = 500;  // TODO - make configurable
  private static final PrioritySchedulerService MONITOR_SCHEDULER;
  private static final ConcurrentMap<List<AuroraServer>, AuroraClusterMonitor> MONITORS;

  static {
    PriorityScheduler ps =
        new PriorityScheduler(4, TaskPriority.High, 1000,
                              new ConfigurableThreadFactory("auroraMonitor-", false, true,
                                                            Thread.NORM_PRIORITY, null, null));
    ps.prestartAllThreads();
    ps.setPoolSize(64); // likely way bigger than will ever be needed
    MONITOR_SCHEDULER = ps;

    MONITORS = new ConcurrentHashMap<>();
  }

  /**
   * Return a monitor instance for a given set of servers.  This instance will be consistent as
   * long as an equivalent servers set is provided.
   *
   * @param servers Set of servers within the cluster
   * @return Monitor to select healthy and well balanced cluster servers
   */
  public static AuroraClusterMonitor getMonitor(Set<AuroraServer> servers) {
    AuroraClusterMonitor result = MONITORS.get(servers);
    if (result != null) {
      return result;
    }
    // must sort servers to ensure list comparison is deterministic
    List<AuroraServer> sortedServers = new ArrayList<>(servers);
    Collections.sort(sortedServers);

    return MONITORS.computeIfAbsent(sortedServers,
                                    (s) -> new AuroraClusterMonitor(MONITOR_SCHEDULER,
                                                                    CHECK_FREQUENCY_MILLIS, servers));
  }

  protected final PrioritySchedulerService scheduler;
  private final ClusterChecker clusterStateChecker;
  private final AtomicLong replicaIndex;  // used to distribute replica reads

  protected AuroraClusterMonitor(PrioritySchedulerService scheduler, long checkIntervalMillis,
                                 Set<AuroraServer> clusterServers) {
    this.scheduler = scheduler;
    clusterStateChecker = new ClusterChecker(scheduler, checkIntervalMillis, clusterServers);
    replicaIndex = new AtomicLong();
  }

  /**
   * Return a random read only replica server.
   *
   * @return Random read only replica server, or {@code null} if no servers are healthy
   */
  public AuroraServer getRandomReadReplica() {
    long replicaIndex = this.replicaIndex.getAndIncrement();
    int secondaryCount;
    // may loop if secondary server becomes unhealthy during check
    while ((secondaryCount = clusterStateChecker.secondaryServers.size()) > 0) {
      try {
        return clusterStateChecker.secondaryServers.get((int)(replicaIndex % secondaryCount));
      } catch (IndexOutOfBoundsException e) {
        // secondary server was removed during check, loop and retry
      }
    }
    return null;
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

  // TODO - Connections need to call into this after error occurs
  /**
   * Indicate a given server should be expedited in checking its current state.  If a given
   * connection finds issues with a suggested server informing the monitor of the issues can ensure
   * that everyone using this cluster can be informed of the state quicker.
   *
   * @param auroraServer Server identifier to be checked
   */
  public void expiditeServerCheck(AuroraServer auroraServer) {
    ServerMonitor monitor = clusterStateChecker.allServers.get(auroraServer);
    if (monitor != null) {
      scheduler.execute(monitor, TaskPriority.Low);
    }
  }

  /**
   * Checks across the state of all servers as they change to see which are potential primaries and
   * which are potential read only slaves.  The use of {@link ReschedulingOperation} with a delay
   * of {@code 0} allows us to ensure this runs as soon as possible when state changes, not run
   * concurrently, and also ensure that we always check the state after modifications have finished
   * (and thus will witness the final cluster state).
   */
  protected class ClusterChecker extends ReschedulingOperation {
    private final Map<AuroraServer, ServerMonitor> allServers;
    private final List<AuroraServer> secondaryServers;
    private final AtomicReference<AuroraServer> masterServer;

    protected ClusterChecker(PrioritySchedulerService scheduler, long checkIntervalMillis,
                             Set<AuroraServer> clusterServers) {
      super(scheduler, 0);

      allServers = new HashMap<>();
      secondaryServers = new CopyOnWriteArrayList<>();
      masterServer = new AtomicReference<>();

      for (AuroraServer server : clusterServers) {
        ServerMonitor monitor = new ServerMonitor(server, this);
        allServers.put(server, monitor);

        if (masterServer.get() == null) {  // check in thread till we find the master
          monitor.run();
          if (! monitor.isReadOnly()) {
            masterServer.set(server);
          }
        } else {  // all other checks can be async, adding in as secondary servers as they complete
          scheduler.execute(monitor, TaskPriority.Low);
        }

        scheduler.scheduleAtFixedRate(monitor,
                                      // hopefully well distributed hash code will distribute
                                      // these tasks so that they are not all checked at once
                                      Math.abs(System.identityHashCode(monitor)) % checkIntervalMillis,
                                      checkIntervalMillis);
      }
    }

    protected void checkAllServers() {
      for (ServerMonitor sm : allServers.values()) {
        scheduler.execute(sm, TaskPriority.Low);
      }
    }

    @Override
    protected void run() {
      for (Map.Entry<AuroraServer, ServerMonitor> p : allServers.entrySet()) {
        if (p.getValue().isHealthy()) {
          if (p.getValue().isReadOnly()) {
            if (p.getKey().equals(masterServer.get())) {
              // no longer a master, will need to find the new one
              masterServer.compareAndSet(p.getKey(), null);
              checkAllServers();  // make sure we find a new master ASAP
            }
            if (! secondaryServers.contains(p.getKey())) {
              secondaryServers.add(p.getKey());
            }
          } else {
            if (! p.getKey().equals(masterServer.get())) {
              // either already was the master, or now is
              masterServer.set(p.getKey());
              log.info("New master server: " + p.getKey());
            }
          }
        } else {
          if (secondaryServers.remove(p.getKey())) {
            // was secondary, so done
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
    private final AuroraServer server;
    private final Connection serverConnection;
    private final ReschedulingOperation clusterStateChecker;
    private final AtomicBoolean running;
    private volatile boolean healthy = false;
    private volatile boolean readOnly = true;

    protected ServerMonitor(AuroraServer server, ReschedulingOperation clusterStateChecker) {
      this.server = server;
      try {
        serverConnection = DelegateDriver.connect(server.hostAndPortString() +
                                                    "/?connectTimeout=10000&socketTimeout=10000&serverTimezone=UTC",
                                                  server.getProperties());
      } catch (SQLException e) {
        throw new RuntimeException("Could not connect to monitor cluster member: " +
                                     server + ", error is fatal", e);
      }
      this.clusterStateChecker = clusterStateChecker;
      this.running = new AtomicBoolean();
    }

    public boolean isHealthy() {
      return healthy;
    }

    public boolean isReadOnly() {
      return readOnly;
    }

    @Override
    public void run() {
      // if already running ignore start
      if (running.compareAndSet(false, true)) {
        try {
          updateState();
        } finally {
          running.set(false);
        }
      }
    }

    protected void updateState() {
      boolean currentlyReadOnly = true;
      boolean currentlyHealthy = false;
      try {
        try (PreparedStatement ps =
                 serverConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';")) {
          try (ResultSet results = ps.executeQuery()) {
            if (results.next()) {
              // unless exactly "OFF" database will be considered read only
              String readOnlyStr = results.getString("Value");
              if (readOnlyStr.equals("OFF")) {
                readOnly = false;
              } else if (readOnlyStr.equals("ON")) {
                readOnly = true;
              } else {
                log.error("Unknown db state, may require library upgrade: " + readOnlyStr);
              }
            } else {
              log.error("No result looking up db state, likely not connected to Aurora database");
            }
          }
        }
        currentlyHealthy = true;
      } catch (Throwable t) {
        log.warn("Setting aurora server " + server + " as unhealthy due to error checking state", t);
        currentlyHealthy = false;
      } finally {
        if (currentlyReadOnly != readOnly || currentlyHealthy != healthy) {
          healthy = currentlyHealthy;
          readOnly = currentlyReadOnly;
          clusterStateChecker.signalToRun();
        }
      }
    }
  }
}
