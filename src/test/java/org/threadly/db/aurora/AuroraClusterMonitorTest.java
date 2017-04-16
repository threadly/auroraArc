package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;

public class AuroraClusterMonitorTest {
  private static final int HOST_START_NUM = 1;
  private static final int HOST_END_NUM = 4;
  private static final int MASTER_HOST = 1;
  
  private MockDriver mockDriver;

  @Before
  public void setup() {
    mockDriver = DelegateMockDriver.setupMockDriverAsDelegate();
  }

  @After
  public void cleanup() {
    DelegateMockDriver.resetDriver();
    mockDriver = null;
    AuroraClusterMonitor.MONITORS.clear();
  }

  private Set<AuroraServer> makeDummySet() {
    Set<AuroraServer> result = new HashSet<>();
    for (int i = HOST_START_NUM; i <= HOST_END_NUM; i++) {
      if (i != MASTER_HOST) {
        result.add(new AuroraServer("host" + i, new Properties()));
      }
    }
    // add master last
    result.add(new AuroraServer("host" + MASTER_HOST, new Properties()));
    return result;
  }

  @Test
  public void getMonitorConsistentResultTest() {
    AuroraClusterMonitor monitor1 = AuroraClusterMonitor.getMonitor(makeDummySet());
    AuroraClusterMonitor monitor2 = AuroraClusterMonitor.getMonitor(makeDummySet());
    assertTrue(monitor1 == monitor2);
  }
  
  private void prepareNormalClusterState() throws SQLException {
    PreparedStatement masterPreapredStatement = mock(PreparedStatement.class);
    ResultSet masterResult = mock(ResultSet.class);
    when(masterPreapredStatement.executeQuery()).thenReturn(masterResult);
    when(masterResult.next()).thenReturn(true);
    when(masterResult.getString("Value")).thenReturn("OFF");
    
    Connection host1MockConnection = mockDriver.getConnectionForHost("host" + MASTER_HOST);
    when(host1MockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
      .thenReturn(masterPreapredStatement);

    PreparedStatement slavePreapredStatement = mock(PreparedStatement.class);
    ResultSet slaveResult = mock(ResultSet.class);
    when(slavePreapredStatement.executeQuery()).thenReturn(slaveResult);
    when(slaveResult.next()).thenReturn(true);
    when(slaveResult.getString("Value")).thenReturn("ON");

    for (int i = HOST_START_NUM; i <= HOST_END_NUM; i++) {
      if (i != MASTER_HOST) {
        Connection hostMockConnection = mockDriver.getConnectionForHost("host" + i);
        when(hostMockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
          .thenReturn(slavePreapredStatement);
      }
    }
  }

  @Test
  public void getRandomReadReplicaTest() throws SQLException {
    prepareNormalClusterState();
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(makeDummySet());
    
    AuroraServer lastServer = monitor.getRandomReadReplica();
    for (int i = 0; i < 10; i++) {
      AuroraServer as = monitor.getRandomReadReplica();
      assertNotNull(as);
      assertNotEquals(lastServer, as);
      assertFalse(as.hostAndPortString().contains("host1"));
      lastServer = as;
    }
  }

  @Test
  public void getRandomReadReplicaMasterOnlyTest() throws SQLException {
    PreparedStatement masterPreapredStatement = mock(PreparedStatement.class);
    ResultSet masterResult = mock(ResultSet.class);
    when(masterPreapredStatement.executeQuery()).thenReturn(masterResult);
    when(masterResult.next()).thenReturn(true);
    when(masterResult.getString("Value")).thenReturn("OFF");
    
    Connection host1MockConnection = mockDriver.getConnectionForHost("host" + MASTER_HOST);
    when(host1MockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
      .thenReturn(masterPreapredStatement);

    for (int i = HOST_START_NUM; i <= HOST_END_NUM; i++) {
      if (i != MASTER_HOST) {
        Connection hostMockConnection = mockDriver.getConnectionForHost("host" + i);
        when(hostMockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
          .thenThrow(SQLException.class);
      }
    }
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(makeDummySet());
    
    assertNull(monitor.getRandomReadReplica());
    assertTrue(monitor.getCurrentMaster().hostAndPortString().contains("host" + MASTER_HOST));
  }

  @Test
  public void getCurrentMasterTest() throws SQLException {
    prepareNormalClusterState();
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(makeDummySet());
    
    assertTrue(monitor.getCurrentMaster().hostAndPortString().contains("host" + MASTER_HOST));
  }
}
