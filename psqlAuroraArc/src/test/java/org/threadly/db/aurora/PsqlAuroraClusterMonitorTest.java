package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;
import org.threadly.db.aurora.psql.PsqlDelegateDriver;
import org.threadly.util.Pair;

public class PsqlAuroraClusterMonitorTest {
  private static final int MASTER_HOST = 1;
  private static final String DOMAIN = ".aws.com";
  
  private Pair<MockDriver, DelegateAuroraDriver> mockDriver;

  public static Pair<MockDriver, DelegateAuroraDriver> setupMockDriverAsDelegate() {
    DelegateMockDriver.class.getName(); // make sure it's loaded
    
    MockDriver md = new MockDriver();
    DelegateAuroraDriver dd = new PsqlDelegateDriver(md);
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
    AuroraServer[] result = new AuroraServer[4];
    for (int i = 0; i < result.length; i++) {
      result[result.length - 1 - i] = new AuroraServer("host" + (i + 1) + DOMAIN, mockDriver.getRight(), new Properties());
    }
    return result;
  }
  
  private void prepareNormalClusterState() throws SQLException {
    for (int i = 1; i <= 4; i++) {
      PreparedStatement preapredStatement = mock(PreparedStatement.class);
      ResultSet resultSet = mock(ResultSet.class);
      when(resultSet.getString("server_id")).thenReturn("host2", "host1", "host3", "host4");
      if (i == MASTER_HOST) {
        when(resultSet.getString("session_id")).thenReturn("MASTER_SESSION_ID");
      } else {
        when(resultSet.getString("session_id")).thenReturn(UUID.randomUUID().toString());
      }
      when(resultSet.next()).thenReturn(true, true, true, true, false);
      when(preapredStatement.executeQuery()).thenReturn(resultSet);
      Connection hostMockConnection = mockDriver.getLeft().getConnectionForHost("host" + i + DOMAIN);
      when(hostMockConnection.prepareStatement("SELECT server_id, session_id FROM aurora_replica_status();"))
        .thenReturn(preapredStatement);
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
      assertFalse(as.hostAndPortString().startsWith("host" + MASTER_HOST + DOMAIN));
      lastServer = as;
    }
  }

  @Test
  public void getCurrentMasterTest() throws SQLException {
    prepareNormalClusterState();
    
    AuroraClusterMonitor monitor = AuroraClusterMonitor.getMonitor(mockDriver.getRight(), makeDummySet());
    
    assertTrue(monitor.getCurrentMaster().hostAndPortString().contains("host" + MASTER_HOST + DOMAIN));
  }
}
