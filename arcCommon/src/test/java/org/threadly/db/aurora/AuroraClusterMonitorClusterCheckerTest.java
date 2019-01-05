package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.db.aurora.AuroraClusterMonitor.ClusterChecker;
import org.threadly.db.aurora.AuroraClusterMonitor.ServerMonitor;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.ExceptionHandler;

public class AuroraClusterMonitorClusterCheckerTest {
  private static final TestableScheduler SERVER_MONITOR_SCHEDULER;
  private static final Map<AuroraServer, ServerMonitor> CLUSTER_SERVERS;
  
  static {
    SERVER_MONITOR_SCHEDULER = new TestableScheduler();
    CLUSTER_SERVERS = new HashMap<>();
    DelegateAuroraDriver dDriver = mock(DelegateAuroraDriver.class);
    try {
      when(dDriver.isMasterServer(any(AuroraServer.class), any())).thenThrow(new NullPointerException());
    } catch (SQLException e) {
      // not possible
    }
    AuroraServer testServer1 = new AuroraServer("host1", new Properties());
    CLUSTER_SERVERS.put(testServer1, new TestServerMonitor(dDriver, SERVER_MONITOR_SCHEDULER, testServer1));
    AuroraServer testServer2 = new AuroraServer("host2", new Properties());
    CLUSTER_SERVERS.put(testServer2, new TestServerMonitor(dDriver, SERVER_MONITOR_SCHEDULER, testServer2));
  }
  
  private TestableScheduler testScheduler;
  private ClusterChecker clusterChecker;
  
  @Before
  public void setup() {
    testScheduler = new TestableScheduler();
    clusterChecker = new ClusterChecker(testScheduler, CLUSTER_SERVERS);
    SERVER_MONITOR_SCHEDULER.clearTasks();
    for (ServerMonitor sm : CLUSTER_SERVERS.values()) {
      ((TestServerMonitor)sm).resetState();
    }
  }
  
  @After
  public void cleanup() {
    testScheduler = null;
    clusterChecker = null;
  }
  
  @Test
  public void updateServerCheckDelayMillisTest() {
    for (ServerMonitor sm : CLUSTER_SERVERS.values()) {
      clusterChecker.scheduleMonitor(sm, 500);
    }
    
    clusterChecker.updateServerCheckDelayMillis(100);
    
    assertEquals(CLUSTER_SERVERS.size(), 
                 testScheduler.advance(100, ExceptionHandler.IGNORE_HANDLER));
  }
  
  @Test
  public void expediteServerCheckTest() {
    AuroraServer testServer = new AuroraServer("host1", new Properties());
    clusterChecker.expediteServerCheck(testServer);
    
    assertEquals(1, clusterChecker.serversWaitingExpeditiedCheck.size());
    assertEquals(1, testScheduler.tick());  // the one expedited check
    
    assertFalse(CLUSTER_SERVERS.get(testServer).isHealthy()); // NPE should have made unhealthy
  }
  
  @Test
  public void expediteUnknownServerCheckTest() {
    clusterChecker.expediteServerCheck(new AuroraServer("missingFooHost", new Properties()));

    assertEquals(0, clusterChecker.serversWaitingExpeditiedCheck.size());
    assertEquals(0, SERVER_MONITOR_SCHEDULER.tick());
  }
  
  @Test
  public void checkAllServersTest() {
    clusterChecker.checkAllServers();
    
    assertEquals(CLUSTER_SERVERS.size(), clusterChecker.serversWaitingExpeditiedCheck.size());
    assertEquals(CLUSTER_SERVERS.size(), testScheduler.tick());

    for (ServerMonitor sm : CLUSTER_SERVERS.values()) {
      assertFalse(sm.isHealthy()); // NPE should have made unhealthy
    }
  }
  
  private static class TestServerMonitor extends ServerMonitor {
    protected TestServerMonitor(DelegateAuroraDriver driver, SubmitterScheduler scheduler, AuroraServer server) {
      super(driver, server, new TestReschedulingOperation(scheduler));
    }
    
    protected void reconnect() throws SQLException {
      // ignore
    }
    
    public void resetState() {
      super.lastError = null;
    }
  }
  
  private static class TestReschedulingOperation extends ReschedulingOperation {
    protected TestReschedulingOperation(SubmitterScheduler scheduler) {
      super(scheduler, 0);
    }

    @Override
    protected void run() {
      // ignored
    }
  }
}
