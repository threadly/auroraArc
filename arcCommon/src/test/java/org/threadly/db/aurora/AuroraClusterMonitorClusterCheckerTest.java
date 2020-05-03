package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

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
  private static final DelegateAuroraDriver MOCK_DRIVER;
  
  static {
    SERVER_MONITOR_SCHEDULER = new TestableScheduler();
    MOCK_DRIVER = mock(DelegateAuroraDriver.class);
    try {
      when(MOCK_DRIVER.isMasterServer(any(AuroraServer.class), any())).thenThrow(new NullPointerException());
    } catch (SQLException e) {
      // not possible
    }
  }
  
  private TestableScheduler testScheduler;
  private ClusterChecker clusterChecker;
  private Map<AuroraServer, ServerMonitor> clusterServers;
  
  @Before
  public void setup() {
    testScheduler = new TestableScheduler();
    SERVER_MONITOR_SCHEDULER.clearTasks();
    
    clusterServers = new HashMap<>();
    AuroraServer testServer1 = new AuroraServer("host1", new Properties());
    clusterServers.put(testServer1, new TestServerMonitor(MOCK_DRIVER, testScheduler, 
                                                          SERVER_MONITOR_SCHEDULER, testServer1));
    AuroraServer testServer2 = new AuroraServer("host2", new Properties());
    clusterServers.put(testServer2, new TestServerMonitor(MOCK_DRIVER, testScheduler, 
                                                          SERVER_MONITOR_SCHEDULER, testServer2));
    
    clusterChecker = new ClusterChecker(testScheduler, clusterServers);
    for (ServerMonitor sm : clusterServers.values()) {
      ((TestServerMonitor)sm).resetState();
    }
    clusterChecker.run();
  }
  
  @After
  public void cleanup() {
    testScheduler = null;
    clusterChecker = null;
  }
  
  @Test
  public void updateServerCheckDelayMillisTest() {
    for (ServerMonitor sm : clusterServers.values()) {
      clusterChecker.scheduleMonitor(sm, 10_000);
    }
    
    clusterChecker.updateServerCheckDelayMillis(1_000);
    
    assertEquals(clusterServers.size(), 
                 testScheduler.advance(1_000, ExceptionHandler.IGNORE_HANDLER));
  }
  
  @Test
  public void expediteServerCheckTest() {
    AuroraServer testServer = new AuroraServer("host1", new Properties());
    clusterChecker.expediteServerCheck(testServer);
    
    assertEquals(1, clusterChecker.serversWaitingExpeditiedCheck.size());
    assertEquals(1, testScheduler.tick());  // the one expedited check
    
    assertFalse(clusterServers.get(testServer).isHealthy()); // NPE should have made unhealthy
  }
  
  @Test
  public void scheduledCheckHealthFailure() {
    for (ServerMonitor sm : clusterServers.values()) {
      testScheduler.schedule(sm, 500);  // don't use scheduleMonitor to avoid random delay
    }
    
    assertEquals(2, testScheduler.advance(500));

    // Nothing should be unhealthy yet, first failure on regular check ignored, prefer failure from connection use instead
    for (ServerMonitor sm : clusterServers.values()) {
      assertNotNull(sm.lastError);
      assertNull(sm.stateError);
      assertTrue(sm.isHealthy()); // NPE should have made unhealthy
    }
    
    assertEquals(2, testScheduler.advance(250));  // check at half the time
    
    // should now be unhealthy due to second NPE failure
    for (ServerMonitor sm : clusterServers.values()) {
      assertFalse(sm.isHealthy());
    }
  }
  
  @Test
  public void setExceptionHandlerTest() {
    AtomicInteger errorCount = new AtomicInteger();
    AuroraClusterMonitor.setExceptionHandler((server, e) -> errorCount.incrementAndGet());
    
    for (ServerMonitor sm : clusterServers.values()) {
      testScheduler.schedule(sm, 500);  // don't use scheduleMonitor to avoid random delay
    }
    
    assertEquals(2, testScheduler.advance(500));
    assertEquals(2, errorCount.get());
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
    
    assertEquals(clusterServers.size(), clusterChecker.serversWaitingExpeditiedCheck.size());
    assertEquals(clusterServers.size(), testScheduler.tick());

    for (ServerMonitor sm : clusterServers.values()) {
      assertFalse(sm.isHealthy()); // NPE should have made unhealthy
    }
  }
  
  @Test
  public void ignoreSetReplicaWeightToOneTest() {
    clusterChecker.queueReplicaWeightUpdate("host1", 3306, 1);
    assertEquals(1, clusterChecker.pendingServerWeightUpdates.size());
    assertEquals(1, testScheduler.advance(500));
    assertEquals(0, clusterChecker.pendingServerWeightUpdates.size());

    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(2, clusterChecker.weightedSecondaryServers.size());
  }
  
  @Test
  public void setOneReplicaWeightToZeroTest() {
    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(2, clusterChecker.weightedSecondaryServers.size());

    clusterChecker.queueReplicaWeightUpdate("host2", 3306, 0);
    assertEquals(1, clusterChecker.pendingServerWeightUpdates.size());
    assertEquals(1, testScheduler.advance(500));
    assertEquals(0, clusterChecker.pendingServerWeightUpdates.size());
    
    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(1, clusterChecker.weightedSecondaryServers.size());
  }
  
  @Test
  public void setAllReplicaWeightToZeroTest() {
    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(2, clusterChecker.weightedSecondaryServers.size());

    clusterChecker.queueReplicaWeightUpdate("host1", 3306, 0);
    clusterChecker.queueReplicaWeightUpdate("host2", 3306, 0);
    assertEquals(2, clusterChecker.pendingServerWeightUpdates.size());
    assertEquals(1, testScheduler.advance(500));
    assertEquals(0, clusterChecker.pendingServerWeightUpdates.size());
    
    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(0, clusterChecker.weightedSecondaryServers.size());
    assertEquals(0, clusterChecker.secondaryServers.get(0).getWeight());
    assertEquals(0, clusterChecker.secondaryServers.get(1).getWeight());
  }
  
  @Test
  public void doubleOneReplicaWeightTest() {
    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(2, clusterChecker.weightedSecondaryServers.size());

    clusterChecker.queueReplicaWeightUpdate("host1", 3306, 2);
    assertEquals(1, clusterChecker.pendingServerWeightUpdates.size());
    assertEquals(1, testScheduler.advance(500));
    assertEquals(0, clusterChecker.pendingServerWeightUpdates.size());
    
    assertEquals(2, clusterChecker.secondaryServers.size());
    assertEquals(3, clusterChecker.weightedSecondaryServers.size());
  }
  
  private static class TestServerMonitor extends ServerMonitor {
    protected TestServerMonitor(DelegateAuroraDriver driver, SubmitterScheduler monitorScheduler, 
                                SubmitterScheduler operationScheduler, AuroraServer server) {
      super(monitorScheduler, driver, server, new TestReschedulingOperation(operationScheduler));
    }
    
    protected void reconnect() {
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
