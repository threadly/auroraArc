package org.threadly.db.aurora;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.db.aurora.AuroraClusterMonitor.ClusterChecker;
import org.threadly.db.aurora.AuroraClusterMonitor.ServerMonitor;
import org.threadly.test.concurrent.TestableScheduler;

@Fork(2)
@Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@SuppressWarnings("unused")
public class AuroraClusterMonitorJMH {
  private static final TestableScheduler TEST_SCHEDULER;
  private static final ClusterChecker NOOP_CLUSTER_CHECKER;
  private static final ClusterChecker WEIGHTED_CLUSTER_CHECKER;
  private static final AuroraClusterMonitor WEIGHTED_MONITOR;
  private static final ClusterChecker UNWEIGHTED_CLUSTER_CHECKER;
  private static final AuroraClusterMonitor UNWEIGHTED_MONITOR;
  private static final ClusterChecker ZEROWEIGHTED_CLUSTER_CHECKER;
  private static final AuroraClusterMonitor ZEROWEIGHTED_MONITOR;
  
  static {
    TEST_SCHEDULER = new TestableScheduler();

    DelegateAuroraDriver masterMockDriver = mock(DelegateAuroraDriver.class);
    DelegateAuroraDriver replicaMockDriver = mock(DelegateAuroraDriver.class);
    try {
      when(masterMockDriver.isMasterServer(any(AuroraServer.class), any())).thenReturn(true);
      when(replicaMockDriver.isMasterServer(any(AuroraServer.class), any())).thenReturn(false);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    
    Map<AuroraServer, ServerMonitor> weightedServers = new HashMap<>();
    AuroraServer masterServer = new AuroraServer("masterHost", new Properties());
    weightedServers.put(masterServer, 
                        new TestServerMonitor(masterMockDriver, TEST_SCHEDULER, masterServer));
    AuroraServer w0Server = new AuroraServer("replicaHostW0", new Properties());
    weightedServers.put(w0Server, 
                        new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, w0Server));
    AuroraServer w1Server = new AuroraServer("replicaHostW1", new Properties());
    weightedServers.put(w1Server, 
                        new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, w1Server));
    AuroraServer w2Server = new AuroraServer("replicaHostW2", new Properties());
    weightedServers.put(w2Server, 
                        new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, w2Server));
    AuroraServer w10Server = new AuroraServer("replicaHostW10", new Properties());
    weightedServers.put(w10Server, 
                        new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, w10Server));
    
    NOOP_CLUSTER_CHECKER = new ClusterChecker(TEST_SCHEDULER, weightedServers);
    WEIGHTED_CLUSTER_CHECKER = new ClusterChecker(TEST_SCHEDULER, weightedServers);
    WEIGHTED_MONITOR = new AuroraClusterMonitor(WEIGHTED_CLUSTER_CHECKER);

    NOOP_CLUSTER_CHECKER.queueReplicaWeightUpdate(w10Server.getHost(), w10Server.getPort(), 10);
    NOOP_CLUSTER_CHECKER.queueReplicaWeightUpdate(w2Server.getHost(), w2Server.getPort(), 2);
    NOOP_CLUSTER_CHECKER.queueReplicaWeightUpdate(w1Server.getHost(), w1Server.getPort(), 1);
    NOOP_CLUSTER_CHECKER.queueReplicaWeightUpdate(w0Server.getHost(), w0Server.getPort(), 0);
    WEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(w10Server.getHost(), w10Server.getPort(), 10);
    WEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(w2Server.getHost(), w2Server.getPort(), 2);
    WEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(w1Server.getHost(), w1Server.getPort(), 1);
    WEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(w0Server.getHost(), w0Server.getPort(), 0);

    Map<AuroraServer, ServerMonitor> unweightedServers = new HashMap<>();
    unweightedServers.put(masterServer, 
                          new TestServerMonitor(masterMockDriver, TEST_SCHEDULER, masterServer));
    AuroraServer replica1Server = new AuroraServer("replicaHost1", new Properties());
    unweightedServers.put(replica1Server, 
                          new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, replica1Server));
    AuroraServer replica2Server = new AuroraServer("replicaHost2", new Properties());
    unweightedServers.put(replica2Server, 
                          new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, replica2Server));
    AuroraServer replica3Server = new AuroraServer("replicaHost3", new Properties());
    unweightedServers.put(replica3Server, 
                          new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, replica3Server));
    AuroraServer replica4Server = new AuroraServer("replicaHost4", new Properties());
    unweightedServers.put(replica4Server, 
                          new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, replica4Server));
    
    UNWEIGHTED_CLUSTER_CHECKER = new ClusterChecker(TEST_SCHEDULER, unweightedServers);
    UNWEIGHTED_MONITOR = new AuroraClusterMonitor(UNWEIGHTED_CLUSTER_CHECKER);

    Map<AuroraServer, ServerMonitor> zeroweightedServers = new HashMap<>();
    zeroweightedServers.put(masterServer, 
                            new TestServerMonitor(masterMockDriver, TEST_SCHEDULER, masterServer));
    AuroraServer zeroReplica1Server = new AuroraServer("zeroReplicaHost1", new Properties());
    zeroweightedServers.put(zeroReplica1Server, 
                            new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, zeroReplica1Server));
    AuroraServer zeroReplica2Server = new AuroraServer("zeroReplicaHost2", new Properties());
    zeroweightedServers.put(zeroReplica2Server, 
                            new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, zeroReplica2Server));
    AuroraServer zeroReplica3Server = new AuroraServer("zeroReplicaHost3", new Properties());
    zeroweightedServers.put(zeroReplica3Server, 
                            new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, zeroReplica3Server));
    AuroraServer zeroReplica4Server = new AuroraServer("zeroReplicaHost4", new Properties());
    zeroweightedServers.put(zeroReplica4Server, 
                            new TestServerMonitor(replicaMockDriver, TEST_SCHEDULER, zeroReplica4Server));
    
    ZEROWEIGHTED_CLUSTER_CHECKER = new ClusterChecker(TEST_SCHEDULER, zeroweightedServers);
    ZEROWEIGHTED_MONITOR = new AuroraClusterMonitor(UNWEIGHTED_CLUSTER_CHECKER);
    
    ZEROWEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(zeroReplica1Server.getHost(), zeroReplica1Server.getPort(), 0);
    ZEROWEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(zeroReplica2Server.getHost(), zeroReplica2Server.getPort(), 0);
    ZEROWEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(zeroReplica3Server.getHost(), zeroReplica3Server.getPort(), 0);
    ZEROWEIGHTED_CLUSTER_CHECKER.queueReplicaWeightUpdate(zeroReplica4Server.getHost(), zeroReplica4Server.getPort(), 0);
    
    WEIGHTED_CLUSTER_CHECKER.run();
    UNWEIGHTED_CLUSTER_CHECKER.run();
    ZEROWEIGHTED_CLUSTER_CHECKER.run();
    TEST_SCHEDULER.tick();   // setup monitors
  }
  
  //@Test // uncomment to run jmh test through junit
  public void run() throws Exception {
    org.openjdk.jmh.Main.main(new String[] { AuroraClusterMonitorJMH.class.getName() });
  }
  
  @Benchmark
  @Group("updateCluster")
  public void noOpUpdateRun() {
    NOOP_CLUSTER_CHECKER.run();
  }
  
  @Benchmark
  @Group("updateCluster")
  public void weightedReplicaUpdate() {
    // clear servers so they can be re-added
    WEIGHTED_CLUSTER_CHECKER.secondaryServers.clear();
    WEIGHTED_CLUSTER_CHECKER.weightedSecondaryServers = Collections.emptyList();
    
    WEIGHTED_CLUSTER_CHECKER.run();
  }
  
  @Benchmark
  @Group("updateCluster")
  public void unweightedReplicaUpdate() {
    // clear servers so they can be re-added
    UNWEIGHTED_CLUSTER_CHECKER.secondaryServers.clear();
    UNWEIGHTED_CLUSTER_CHECKER.weightedSecondaryServers = Collections.emptyList();
    
    UNWEIGHTED_CLUSTER_CHECKER.run();
  }
  
  @Benchmark
  @Group("updateCluster")
  public void zeroWeightReplicaUpdate() {
    // clear servers so they can be re-added
    ZEROWEIGHTED_CLUSTER_CHECKER.secondaryServers.clear();
    ZEROWEIGHTED_CLUSTER_CHECKER.weightedSecondaryServers = Collections.emptyList();
    
    ZEROWEIGHTED_CLUSTER_CHECKER.run();
  }
  
  @Benchmark
  @Group("getServer")
  public void currentMaster() {
    WEIGHTED_MONITOR.getCurrentMaster();
  }
  
  @Benchmark
  @Group("getServer")
  public void weightedRandomReplica() {
    WEIGHTED_MONITOR.getRandomReadReplica();
  }
  
  @Benchmark
  @Group("getServer")
  public void unweightedRandomReplica() {
    UNWEIGHTED_MONITOR.getRandomReadReplica();
  }
  
  @Benchmark
  @Group("getServer")
  public void zeroweightedRandomReplica() {
    ZEROWEIGHTED_MONITOR.getRandomReadReplica();
  }
  
  private static class TestServerMonitor extends ServerMonitor {
    protected TestServerMonitor(DelegateAuroraDriver driver, SubmitterScheduler scheduler, 
                                AuroraServer server) {
      super(scheduler, driver, server, NoOpReschedulingOperation.INSTANCE);
    }
    
    protected void reconnect() {
      // ignore
    }
  }
  
  private static class NoOpReschedulingOperation extends ReschedulingOperation {
    public static final NoOpReschedulingOperation INSTANCE = new NoOpReschedulingOperation();
    
    protected NoOpReschedulingOperation() {
      super(SameThreadSubmitterExecutor.instance());
    }

    @Override
    protected void run() {
      // ignored
    }
  }
}
