package org.threadly.db.aurora;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.util.Pair;

public class AuroraClusterMonitor {
  private static final int CHECK_FREQUENCY_MILLIS = 500;  // TODO - good value?
  private static final PrioritySchedulerService MONITOR_SCHEDULER;
  private static final ConcurrentMap<List<AuroraServer>, AuroraClusterMonitor> MONITORS;
  
  static {
    MONITOR_SCHEDULER = 
        new PriorityScheduler(4, TaskPriority.High, 1000, 
                              new ConfigurableThreadFactory("auroraClusterMonitor-", false, true, 
                                                            Thread.NORM_PRIORITY, null, null));
    MONITORS = new ConcurrentHashMap<>();
  }
  
  public static AuroraClusterMonitor getMonitor(List<AuroraServer> servers) {
    AuroraClusterMonitor result = MONITORS.get(servers);
    if (result != null) {
      return result;
    }
    // must sort servers to ensure list comparison is deterministic
    List<AuroraServer> sortedServers = new ArrayList<>(servers);
    Collections.sort(sortedServers, new Comparator<AuroraServer>() {
      @Override
      public int compare(AuroraServer o1, AuroraServer o2) {
        // TODO - figure out fast but consistent way to sort
        return 0;
      }
    });
    if (sortedServers.equals(servers)) {  // TODO - optimization make sense?
      // guess we wasted our time, but set the original list to allow faster comparisions in the future
      sortedServers = servers;
    }
    
    return MONITORS.computeIfAbsent(sortedServers, (s) -> new AuroraClusterMonitor(s));
  }
  
  private final ClusterChecker clusterStateChecker;
  private final AtomicLong replicaIndex;  // used to distribute replica reads
  
  private AuroraClusterMonitor(List<AuroraServer> clusterServers) {
    clusterStateChecker = new ClusterChecker(MONITOR_SCHEDULER, clusterServers);
    replicaIndex = new AtomicLong();
  }

  /**
   * Return a random read only replica server.
   * 
   * @return Random read only replica server, or {@code null} if no servers are healthy
   */
  public AuroraServer getRandomReadReplica() {
    long replicaIndex = this.replicaIndex.getAndIncrement();
    while (true) {  // may loop if secondary server becomes unhealthy during check
      try {
        return clusterStateChecker.secondaryServers
                                  .get((int)(replicaIndex % clusterStateChecker.secondaryServers.size()));
      } catch (IndexOutOfBoundsException e) {
        // secondary server was removed during check, loop and retry
      }
    }
  }

  /**
   * Returns the current cluster master which can accept writes.  May be {@code null} if there is 
   * not currently any healthy cluster members which can accept writes.
   * 
   * @return Current writable server or {@code null} if no known one exists
   */
  public AuroraServer getCurrentMaster() {
    return clusterStateChecker.masterServer;
  }
  
  /**
   * Checks across the state of all servers as they change to see which are potential primaries and 
   * which are potential read only slaves.  The use of {@link ReschedulingOperation} with a delay 
   * of {@code 0} allows us to ensure this runs as soon as possible when state changes, not run 
   * concurrently, and also ensure that we always check the state after modifications have finished 
   * (and thus will witness the final cluster state).
   */
  private class ClusterChecker extends ReschedulingOperation {
    private final List<Pair<AuroraServer, ServerMonitor>> allServers;
    private final List<AuroraServer> secondaryServers;
    private volatile AuroraServer masterServer;
    
    protected ClusterChecker(SubmitterScheduler scheduler, List<AuroraServer> clusterServers) {
      super(scheduler, 0);
      
      allServers = new ArrayList<>(clusterServers.size());
      boolean masterFound = false;  // we check in thread until we find the master
      for (AuroraServer server : clusterServers) {
        ServerMonitor monitor = new ServerMonitor();
        if (masterFound) {
          MONITOR_SCHEDULER.execute(monitor, TaskPriority.Low);
        } else {
          monitor.run();
          masterFound = ! monitor.isReadOnly();
        }
        // TODO - schedule monitor to re-check status, staggered to avoid surges
        allServers.add(new Pair<>(server, monitor));
      }
      secondaryServers = new CopyOnWriteArrayList<>();
    }

    @Override
    protected void run() {
      for (Pair<AuroraServer, ServerMonitor> p : allServers) {
        if (p.getRight().isHealthy()) {
          if (p.getRight().isReadOnly()) {
            if (p.getLeft().equals(masterServer)) {
              // no longer a master, will need to find the new one
              masterServer = null;
            }
            if (! secondaryServers.contains(p.getLeft())) {
              secondaryServers.add(p.getLeft());
            }
          } else {
            // either already was the master, or now is
            masterServer = p.getLeft();
          }
        } else {
          if (secondaryServers.remove(p.getLeft())) {
            // was secondary, so done
          } else if (p.getLeft().equals(masterServer)) {
            // no master till we find a new one
            masterServer = null;
          } // else, server is already known to be unhealthy, no change
        }
      }
    }
  }
  
  // TODO - monitor should run early if sql error occurs
  private class ServerMonitor implements Runnable {
    private volatile boolean healthy = false;
    private volatile boolean readOnly = true;
    
    public boolean isHealthy() {
      return healthy;
    }
    
    public boolean isReadOnly() {
      return readOnly;
    }

    @Override
    public void run() {
      boolean currentlyReadOnly = true;
      boolean currentlyHealthy = false;
      try {
      // TODO - check if server is read only by querying SQL with timeout
        currentlyHealthy = true;
      } catch (Throwable t) {
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
