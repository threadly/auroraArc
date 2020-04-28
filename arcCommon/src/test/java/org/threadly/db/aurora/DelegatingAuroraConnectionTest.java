package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;
import org.threadly.db.aurora.DelegatingAuroraConnection.NoAuroraServerException;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.util.StringUtils;

public class DelegatingAuroraConnectionTest {
  private static final String MASTER_HOST = DelegateMockDriver.MASTER_HOST;
  private static final String REPLICA_HOST = "fooHost";
  
  private MockDriver mockDriver;
  private Connection mockConnection1;
  private Connection mockConnection2;
  private DelegatingAuroraConnection auroraConnection;

  @Before
  public void setup() throws SQLException {
    mockDriver = DelegateMockDriver.setupMockDriverAsDelegate().getLeft();
    mockConnection1 = mockDriver.getConnectionForHost(MASTER_HOST);
    mockConnection2 = mockDriver.getConnectionForHost(REPLICA_HOST);
    auroraConnection = new DelegatingAuroraConnection(DelegateAuroraDriver.getAnyDelegateDriver().getArcPrefix() + 
                                                      MASTER_HOST + "," + REPLICA_HOST + 
                                                        "/auroraArc", 
                                                      new Properties());
    new TestCondition(() -> {
      try {
        auroraConnection.getAuroraServerReplicaOnly();
        return true;
      } catch (NoAuroraServerException e) {
        return false; // wait till cluster is fully initialized
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }).blockTillTrue();
  }

  @After
  public void cleanup() {
    try {
      auroraConnection.close();
    } catch (Exception ignored) { }
    DelegateMockDriver.resetDriver();
    mockDriver = null;
    mockConnection1 = null;
    mockConnection2 = null;
    AuroraClusterMonitor.MONITORS.clear();
  }
  
  @Test
  public void serverDeduplicationTest() throws SQLException {
    cleanup();  // don't use normal test setup

    mockDriver = DelegateMockDriver.setupMockDriverAsDelegate().getLeft();
    auroraConnection = new DelegatingAuroraConnection(DelegateAuroraDriver.getAnyDelegateDriver().getArcPrefix() + 
                                                        "fooHost1,fooHost2,fooHost1,fooHost2,fooHost2" +  
                                                        "/auroraArc", 
                                                      new Properties());
    
    assertEquals(2, auroraConnection.servers.length);
    assertFalse(auroraConnection.servers[0].equals(auroraConnection.servers[1]));
  }
  
  @Test
  public void setClientInfoDelegateDefaultTest() throws SQLClientInfoException {
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART, 
                 DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT);
    
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, null);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT, 
                 auroraConnection.delegateChoice);
    
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, "");
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT, 
                 auroraConnection.delegateChoice);
  }
  
  @Test
  public void getDelegateSmartTest() throws SQLException {
    auroraConnection.setAutoCommit(true);
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT, 
                 auroraConnection.delegateChoice);
    
    assertEquals(MASTER_HOST, auroraConnection.getDelegate().getLeft().getHost());
    
    auroraConnection.setReadOnly(true);
    
    assertEquals(REPLICA_HOST, auroraConnection.getDelegate().getLeft().getHost());
  }
  
  @Test
  public void getDelegateAnyTest() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY, 
                 auroraConnection.delegateChoice);
    
    assertNotNull(auroraConnection.getDelegate());
  }
  
  @Test
  public void getDelegateMasterPrefered() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE,
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED);

    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED,
                 auroraConnection.delegateChoice);
    
    assertEquals(MASTER_HOST, auroraConnection.getDelegate().getLeft().getHost());
  }
  
  @Test
  public void getDelegateMasterOnly() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY, 
                 auroraConnection.delegateChoice);
    
    assertEquals(MASTER_HOST, auroraConnection.getDelegate().getLeft().getHost());
  }
  
  @Test
  public void getDelegateReplicaPefered() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE,
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED);

    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED,
                 auroraConnection.delegateChoice);
    
    assertEquals(REPLICA_HOST, auroraConnection.getDelegate().getLeft().getHost());
  }
  
  @Test
  public void getDelegateReplicaOnly() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY, 
                 auroraConnection.delegateChoice);
    
    assertEquals(REPLICA_HOST, auroraConnection.getDelegate().getLeft().getHost());
  }
  
  @Test
  public void getDelegateFirstHalfReplicaOnly() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY, 
                 auroraConnection.delegateChoice);
    
    assertEquals(REPLICA_HOST, auroraConnection.getDelegate().getLeft().getHost());
  }
  
  @Test (expected = NoAuroraServerException.class)
  public void getDelegateSecondHalfReplicaFail() throws SQLException {
    auroraConnection.setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                   DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_2_REPLICA_ONLY);
    
    assertEquals(DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_2_REPLICA_ONLY, 
                 auroraConnection.delegateChoice);
    
    auroraConnection.getDelegate();
    fail("Exception should have thrown");
  }
  
  @Test
  public void isValidTest() throws SQLException {
    auroraConnection.isValid(1000);
    
    verify(mockConnection1).isValid(1000);
  }
  
  @Test
  public void setCatalogTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    
    auroraConnection.setCatalog(catalog);
    
    verify(mockConnection1).setCatalog(catalog);
    verify(mockConnection2).setCatalog(catalog);
  }
  
  @Test
  public void getCatalogTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    
    when(mockConnection1.getCatalog()).thenReturn(catalog);
    
    assertEquals(catalog, auroraConnection.getCatalog());
  }
  
  @Test
  public void setSchemaTest() throws SQLException {
    String schema = StringUtils.makeRandomString(5);
    
    auroraConnection.setSchema(schema);
    
    verify(mockConnection1).setSchema(schema);
    verify(mockConnection2).setSchema(schema);
  }
  
  @Test
  public void getSchemaTest() throws SQLException {
    String schema = StringUtils.makeRandomString(5);
    
    when(mockConnection1.getSchema()).thenReturn(schema);
    
    assertEquals(schema, auroraConnection.getSchema());
  }
  
  @Test
  public void setHoldabilityTest() throws SQLException {
    auroraConnection.setHoldability(10);
    
    verify(mockConnection1).setHoldability(10);
    verify(mockConnection2).setHoldability(10);
  }
  
  @Test
  public void getHoldabilityTest() throws SQLException {
    when(mockConnection1.getHoldability()).thenReturn(10);
    
    assertEquals(10, auroraConnection.getHoldability());
  }
  
  @Test
  public void setClientInfoTest() throws SQLException {
    String name = StringUtils.makeRandomString(5);
    String value = StringUtils.makeRandomString(5);
    
    auroraConnection.setClientInfo(name, value);
    
    verify(mockConnection1).setClientInfo(name, value);
    verify(mockConnection2).setClientInfo(name, value);
  }
  
  @Test
  public void setClientInfoPropertiesTest() throws SQLException {
    String name = StringUtils.makeRandomString(5);
    String value = StringUtils.makeRandomString(5);
    Properties p = new Properties();
    p.setProperty(name, value);
    
    auroraConnection.setClientInfo(p);
    
    verify(mockConnection1).setClientInfo(p);
    verify(mockConnection2).setClientInfo(p);
  }
  
  @Test
  public void getClientInfoTest() throws SQLException {
    String name = StringUtils.makeRandomString(5);
    String value = StringUtils.makeRandomString(5);
    

    when(mockConnection1.getClientInfo(name)).thenReturn(value);
    
    assertEquals(value, auroraConnection.getClientInfo(name));
  }
  
  @Test
  public void setAndGetAutoCommitTest() throws SQLException {
    auroraConnection.setAutoCommit(true);
    assertTrue(auroraConnection.getAutoCommit());
    auroraConnection.setAutoCommit(false);
    assertFalse(auroraConnection.getAutoCommit());
    
    verify(auroraConnection.getDelegate().getRight().verifiedState()).setAutoCommit(false);
  }
  
  @Test
  public void setAndGetReadOnlyTest() throws SQLException {
    auroraConnection.setReadOnly(true);
    assertTrue(auroraConnection.isReadOnly());
    auroraConnection.setReadOnly(false);
    assertFalse(auroraConnection.isReadOnly());
    
    verify(auroraConnection.getDelegate().getRight().verifiedState()).setReadOnly(false);
  }
  
  @Test
  public void clearWarningsTest() throws SQLException {
    auroraConnection.clearWarnings();

    verify(mockConnection1).clearWarnings();
    verify(mockConnection2).clearWarnings();
  }
  
  @Test
  public void getWarningsTest() throws SQLException {
    SQLWarning warning1 = new SQLWarning("warn1");
    when(mockConnection1.getWarnings()).thenReturn(warning1);
    SQLWarning warning2 = new SQLWarning("warn2");
    when(mockConnection2.getWarnings()).thenReturn(warning2);
    
    SQLWarning result = auroraConnection.getWarnings();
    
    assertEquals(warning1.getMessage(), result.getMessage());
    assertEquals(warning2, result.getNextWarning());
  }
  
  @Test
  public void abortTest() throws SQLException {
    auroraConnection.abort(SameThreadSubmitterExecutor.instance());
    
    assertTrue(auroraConnection.isClosed());
    verify(mockConnection1).abort(SameThreadSubmitterExecutor.instance());
    verify(mockConnection2).abort(SameThreadSubmitterExecutor.instance());
  }
  
  @Test
  public void closeTest() throws SQLException {
    assertFalse(auroraConnection.isClosed());
    auroraConnection.close();

    assertTrue(auroraConnection.isClosed());
    verify(mockConnection1).close();
    verify(mockConnection2).close();
  }
}
