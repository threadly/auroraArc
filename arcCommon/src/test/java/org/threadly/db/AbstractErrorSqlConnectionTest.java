package org.threadly.db;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.ExceptionUtils;

public abstract class AbstractErrorSqlConnectionTest {
  protected static final SQLException ERROR = new SQLException();

  protected TestRunnable testListener;
  protected AbstractErrorSqlConnection connection;
  
  @Before
  public void setup() {
    testListener = new TestRunnable();
    connection = makeConnection(testListener, ERROR);
  }
  
  protected abstract AbstractErrorSqlConnection makeConnection(Runnable testListener, SQLException error);

  @After
  public void cleanup() {
    testListener = null;
    connection = null;
  }

  @Test
  public void closeTest() {
    assertFalse(connection.isClosed());
    
    connection.close();
    
    assertTrue(connection.isClosed());
  }

  @Test
  public void clearWarningsTest() throws SQLException {
    connection.clearWarnings();
    // nothing should throw
  }

  @Test
  public void setClientInfoTest() throws SQLException {
    connection.setClientInfo("key", "value");
    // nothing should throw
  }

  @Test
  public void setAutoCommitTest() throws SQLException {
    connection.setAutoCommit(true);
    // nothing should throw
  }

  @Test
  public void setCatalogTest() throws SQLException {
    connection.setCatalog(null);
    // nothing should throw
  }

  @Test
  public void setHoldabilityTest() throws SQLException {
    connection.setHoldability(-1);
    // nothing should throw
  }

  @Test
  public void setNetworkTimeoutTest() throws SQLException {
    connection.setNetworkTimeout(null, -1);
    // nothing should throw
  }

  @Test
  public void setReadOnlyTest() throws SQLException {
    connection.setReadOnly(true);
    // nothing should throw
  }

  @Test
  public void setSchemaTest() throws SQLException {
    connection.setSchema(null);
    // nothing should throw
  }

  @Test
  public void setTransactionIsolationTest() throws SQLException {
    connection.setTransactionIsolation(-1);
    // nothing should throw
  }

  @Test
  public void setTypeMapTest() throws SQLException {
    connection.setTypeMap(Collections.emptyMap());
    // nothing should throw
  }
  
  protected void verifyAction(Callable<?> operation) {
    try {
      operation.call();
      fail("Exception should have thrown");
    } catch (SQLException e) {
      assertTrue(e.getCause() == ERROR);
      assertTrue(testListener.ranOnce());
    } catch (Exception e) {
      fail("Unexpected error: \n" + ExceptionUtils.stackToString(e));
    }
  }

  @Test
  public void isWrapperForTest() {
    verifyAction(() -> connection.isWrapperFor(null));
  }

  @Test
  public void unwrapTest() {
    verifyAction(() -> connection.unwrap(null));
  }

  @Test
  public void abortTest() {
    verifyAction(() -> { connection.abort(null); return null; });
  }

  @Test
  public void commitTest() {
    verifyAction(() -> { connection.commit(); return null; });
  }

  @Test
  public void createArrayOfTest() {
    verifyAction(() -> connection.createArrayOf(null, null));
  }

  @Test
  public void createBlobTest() {
    verifyAction(connection::createBlob);
  }

  @Test
  public void createClobTest() {
    verifyAction(connection::createClob);
  }

  @Test
  public void createNClobTest() {
    verifyAction(connection::createNClob);
  }

  @Test
  public void createSQLXMLTest() {
    verifyAction(connection::createSQLXML);
  }

  @Test
  public void createStatementTest() {
    verifyAction(connection::createStatement);
  }

  @Test
  public void createStructTest() {
    verifyAction(() -> connection.createStruct(null, null));
  }

  @Test
  public void getAutoCommitTest() {
    verifyAction(connection::getAutoCommit);
  }

  @Test
  public void getCatalogTest() {
    verifyAction(connection::getCatalog);
  }

  @Test
  public void getClientInfoTest() {
    verifyAction(connection::getClientInfo);
  }

  @Test
  public void getHoldabilityTest() {
    verifyAction(connection::getHoldability);
  }

  @Test
  public void getMetaDataTest() {
    verifyAction(connection::getMetaData);
  }

  @Test
  public void getNetworkTimeoutTest() {
    verifyAction(connection::getNetworkTimeout);
  }

  @Test
  public void getSchemaTest() {
    verifyAction(connection::getSchema);
  }

  @Test
  public void getTransactionIsolationTest() {
    verifyAction(connection::getTransactionIsolation);
  }

  @Test
  public void getTypeMapTest() {
    verifyAction(connection::getTypeMap);
  }

  @Test
  public void getWarningsTest() {
    verifyAction(connection::getWarnings);
  }

  @Test
  public void isReadOnlyTest() {
    verifyAction(connection::isReadOnly);
  }

  @Test
  public void nativeSQLTest() {
    verifyAction(() -> connection.nativeSQL(null));
  }

  @Test
  public void prepareCallTest() {
    verifyAction(() -> connection.prepareCall(null));
  }

  @Test
  public void prepareStatementTest() {
    verifyAction(() -> connection.prepareStatement(null));
  }

  @Test
  public void releaseSavepointTest() {
    verifyAction(() -> { connection.releaseSavepoint(null); return null; });
  }

  @Test
  public void rollbackTest() {
    verifyAction(() -> { connection.rollback(); return null; });
  }

  @Test
  public void setSavepointTest() {
    verifyAction(connection::setSavepoint);
  }
}
