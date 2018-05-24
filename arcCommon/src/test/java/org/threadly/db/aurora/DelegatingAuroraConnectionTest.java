package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;
import org.threadly.util.StringUtils;

public class DelegatingAuroraConnectionTest {
  private MockDriver mockDriver;
  private Connection mockConnection1;
  private Connection mockConnection2;
  private DelegatingAuroraConnection auroraConnection;

  @Before
  public void setup() throws SQLException {
    String host = "fooHost";
    mockDriver = DelegateMockDriver.setupMockDriverAsDelegate().getLeft();
    mockConnection1 = mockDriver.getConnectionForHost(host);
    mockConnection2 = mockDriver.getConnectionForHost(host + "2");
    auroraConnection = new DelegatingAuroraConnection(DelegateAuroraDriver.getAnyDelegateDriver().getArcPrefix() + 
                                                        host + "," + host + "2" + 
                                                        "/auroraArc", 
                                                      new Properties());
  }

  @After
  public void cleanup() {
    DelegateMockDriver.resetDriver();
    mockDriver = null;
    mockConnection1 = null;
    mockConnection2 = null;
    AuroraClusterMonitor.MONITORS.clear();
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
    auroraConnection.close();

    assertTrue(auroraConnection.isClosed());
    verify(mockConnection1).close();
    verify(mockConnection2).close();
  }
}
