package org.threadly.db;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.util.ArgumentVerifier;

/**
 * Implementation of {@link Connection} which is perpetually in a state of error.  Any operation on 
 * this connection will result in an exception being thrown.
 * <p>
 * The connection will appear valid (from {@link #isValid(int)} and non-closed UNTIL the exception 
 * is thrown.  After that point the connection will appear as if it was closed.
 * 
 * @since 0.10
 */
abstract class AbstractErrorSqlConnection implements Connection {
  private final Runnable errorThrownListener;
  private final SQLException sqlError;
  private final RuntimeException runtimeError;
  private volatile boolean closed = false;
  private volatile boolean errorThrown = false;
  
  /**
   * Construct a new {@link AbstractErrorSqlConnection}.
   * 
   * @param errorThrownListener Listener to be invoked when error is realized (ie thrown)
   * @param error Error to throw once Connection is attempted to be used
   */
  public AbstractErrorSqlConnection(Runnable errorThrownListener, SQLException error) {
    ArgumentVerifier.assertNotNull(error, "error");
    
    if (errorThrownListener == null) {
      errorThrownListener = DoNothingRunnable.instance();
    }
    
    this.errorThrownListener = errorThrownListener;
    this.sqlError = error;
    this.runtimeError = null;
  }

  /**
   * Construct a new {@link AbstractErrorSqlConnection}.
   * 
   * @param errorThrownListener Listener to be invoked when error is realized (ie thrown)
   * @param error Error to throw once Connection is attempted to be used
   */
  public AbstractErrorSqlConnection(Runnable errorThrownListener, RuntimeException error) {
    ArgumentVerifier.assertNotNull(error, "error");
    
    if (errorThrownListener == null) {
      errorThrownListener = DoNothingRunnable.instance();
    }
    
    this.errorThrownListener = errorThrownListener;
    this.sqlError = null;
    this.runtimeError = error;
  }
  
  protected SQLException error() throws SQLException {
    errorThrown = true;
    errorThrownListener.run();
    
    if (sqlError != null) {
      throw new SQLException(sqlError);
    } else {
      throw new SQLException(runtimeError);
    }
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return errorThrown || closed;
  }

  @Override
  public void setClientInfo(Properties arg0) throws SQLClientInfoException {
    // ignored
  }

  @Override
  public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
    // ignored
  }

  @Override
  public void clearWarnings() throws SQLException {
    // ignored
  }
  
  // set operations ignored as the state of this connection does not matter


  @Override
  public void setAutoCommit(boolean arg0) throws SQLException {
    // ignored
  }

  @Override
  public void setCatalog(String arg0) throws SQLException {
    // ignored
  }

  @Override
  public void setHoldability(int arg0) throws SQLException {
    // ignored
  }

  @Override
  public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
    // ignored
  }

  @Override
  public void setReadOnly(boolean arg0) throws SQLException {
    // ignored
  }

  @Override
  public void setSchema(String arg0) throws SQLException {
    // ignored
  }

  @Override
  public void setTransactionIsolation(int arg0) throws SQLException {
    // ignored
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
    // ignored
  }
  
  // other operations expose the error
  
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw error();
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw error();
  }

  @Override
  public void abort(Executor arg0) throws SQLException {
    throw error();
  }

  @Override
  public void commit() throws SQLException {
    throw error();
  }

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    throw error();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw error();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw error();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw error();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw error();
  }

  @Override
  public Statement createStatement() throws SQLException {
    throw error();
  }

  @Override
  public Statement createStatement(int arg0, int arg1) throws SQLException {
    throw error();
  }

  @Override
  public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
    throw error();
  }

  @Override
  public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
    throw error();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw error();
  }

  @Override
  public String getCatalog() throws SQLException {
    throw error();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw error();
  }

  @Override
  public String getClientInfo(String arg0) throws SQLException {
    throw error();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw error();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throw error();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw error();
  }

  @Override
  public String getSchema() throws SQLException {
    throw error();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw error();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw error();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw error();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw error();
  }

  @Override
  public String nativeSQL(String arg0) throws SQLException {
    throw error();
  }

  @Override
  public CallableStatement prepareCall(String arg0) throws SQLException {
    throw error();
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
    throw error();
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
    throw error();
  }

  @Override
  public PreparedStatement prepareStatement(String arg0) throws SQLException {
    throw error();
  }

  @Override
  public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
    throw error();
  }

  @Override
  public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
    throw error();
  }

  @Override
  public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
    throw error();
  }

  @Override
  public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
    throw error();
  }

  @Override
  public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
    throw error();
  }

  @Override
  public void releaseSavepoint(Savepoint arg0) throws SQLException {
    throw error();
  }

  @Override
  public void rollback() throws SQLException {
    throw error();
  }

  @Override
  public void rollback(Savepoint arg0) throws SQLException {
    throw error();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw error();
  }

  @Override
  public Savepoint setSavepoint(String arg0) throws SQLException {
    throw error();
  }
}
