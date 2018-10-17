package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.db.aurora.AuroraClusterMonitor;
import org.threadly.db.aurora.AuroraServer;
import org.threadly.db.aurora.DelegateAuroraDriver;
import org.threadly.db.aurora.DelegateMockDriver;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;
import org.threadly.db.aurora.mysql.MySqlDelegateDriver;
import org.threadly.util.Pair;

public class MySqlAuroraClusterMonitorTest {
  private static final int HOST_START_NUM = 1;
  private static final int HOST_END_NUM = 4;
  private static final int MASTER_HOST = 1;
  
  private Pair<MockDriver, DelegateAuroraDriver> mockDriver;

  public static Pair<MockDriver, DelegateAuroraDriver> setupMockDriverAsDelegate() {
    DelegateMockDriver.class.getName(); // make sure it's loaded
    
    MockDriver md = new MockDriver();
    DelegateAuroraDriver dd = new MySqlDelegateDriver(md);
    for (int i = 0; i < DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS.length; i++) {
      DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS[i] = 
          new Pair<String, DelegateAuroraDriver>(DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS[i].getLeft(), dd);
    }
    return new Pair<>(md, dd);
  }
  
  @Before
  public void setup() {
    mockDriver = setupMockDriverAsDelegate();
  }

  @After
  public void cleanup() {
    DelegateMockDriver.resetDriver();
    mockDriver = null;
    AuroraClusterMonitor.MONITORS.clear();
  }

  private AuroraServer[] makeDummySet() {
    int index = 0;
    AuroraServer[] result = new AuroraServer[(HOST_END_NUM - HOST_START_NUM) + 1];
    if (ThreadLocalRandom.current().nextBoolean()) {
      for (int i = HOST_START_NUM; i <= HOST_END_NUM; i++) {
        if (i != MASTER_HOST) {
          result[index++] = new AuroraServer("host" + i, new Properties());
        }
      }
    } else {
      for (int i = HOST_END_NUM; i >= HOST_START_NUM; i--) {
        if (i != MASTER_HOST) {
          result[index++] = new AuroraServer("host" + i, new Properties());
        }
      }
    }
    // add master last
    result[index++] = new AuroraServer("host" + MASTER_HOST, new Properties());
    return result;
  }

  @Test
  public void getMonitorConsistentResultTest() {
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(mockDriver.getRight(), makeDummySet());
    for (int i = 0; i < 100; i++) {
      assertTrue(monitor == AuroraClusterMonitor.getMonitor(mockDriver.getRight(), makeDummySet()));
    }
  }
  
  private void prepareNormalClusterState() throws SQLException {
    PreparedStatement masterPreapredStatement = mock(PreparedStatement.class);
    ResultSet masterResult = mock(ResultSet.class);
    when(masterPreapredStatement.executeQuery()).thenReturn(masterResult);
    when(masterResult.next()).thenReturn(true);
    when(masterResult.getString("Value")).thenReturn("OFF");
    
    Connection host1MockConnection = mockDriver.getLeft().getConnectionForHost("host" + MASTER_HOST);
    when(host1MockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
      .thenReturn(masterPreapredStatement);

    PreparedStatement slavePreapredStatement = mock(PreparedStatement.class);
    ResultSet slaveResult = mock(ResultSet.class);
    when(slavePreapredStatement.executeQuery()).thenReturn(slaveResult);
    when(slaveResult.next()).thenReturn(true);
    when(slaveResult.getString("Value")).thenReturn("ON");

    for (int i = HOST_START_NUM; i <= HOST_END_NUM; i++) {
      if (i != MASTER_HOST) {
        Connection hostMockConnection = mockDriver.getLeft().getConnectionForHost("host" + i);
        when(hostMockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
          .thenReturn(slavePreapredStatement);
      }
    }
  }

  @Test
  public void getRandomReadReplicaTest() throws SQLException {
    prepareNormalClusterState();
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(mockDriver.getRight(), makeDummySet());
    
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
    
    Connection host1MockConnection = mockDriver.getLeft().getConnectionForHost("host" + MASTER_HOST);
    when(host1MockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
      .thenReturn(masterPreapredStatement);

    for (int i = HOST_START_NUM; i <= HOST_END_NUM; i++) {
      if (i != MASTER_HOST) {
        Connection hostMockConnection = mockDriver.getLeft().getConnectionForHost("host" + i);
        when(hostMockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
          .thenThrow(SQLException.class);
      }
    }
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(mockDriver.getRight(), makeDummySet());
    
    assertNull(monitor.getRandomReadReplica());
    assertTrue(monitor.getCurrentMaster().hostAndPortString().contains("host" + MASTER_HOST));
  }

  @Test
  public void getCurrentMasterTest() throws SQLException {
    prepareNormalClusterState();
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(mockDriver.getRight(), makeDummySet());
    
    assertTrue(monitor.getCurrentMaster().hostAndPortString().contains("host" + MASTER_HOST));
  }
}
