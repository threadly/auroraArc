package org.threadly.db;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 * Driver mapped with {@code "jdbc:mysql:logging://"} url's that will log out actions occurring.
 * <p>  
 * This is not intended to be used externally, but rather is a tool for debugging and internal 
 * driver development.
 */
public class LoggingDriver extends AbstractArcDriver {
  public static final String URL_PREFIX = "jdbc:mysql:logging://";
  protected static final String DELEGATE_DRIVER_PREFIX;
  protected static final java.sql.Driver DELEGATE_DRIVER;

  static {
    try {
      //DELEGATE_DRIVER_PREFIX = "jdbc:mysql://";
      //DELEGATE_DRIVER = new com.mysql.cj.jdbc.Driver();
      DELEGATE_DRIVER_PREFIX = "jdbc:mysql:aurora://";
      DELEGATE_DRIVER = new org.threadly.db.aurora.Driver();

      DriverManager.registerDriver(new LoggingDriver());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Another way to register the driver.  This is more convenient than `Class.forName(String)` as 
   * no exceptions need to be handled (instead just relying on the compile time dependency).
   */
  public static void registerDriver() {
    // Nothing needed, just a nicer way to initialize the static registration compared to Class.forName.
  }
  
  private final String logPrefix = Integer.toHexString(System.identityHashCode(this)) + "-SqlDriver> ";
  
  protected void log(String msg) {
    System.out.println(logPrefix + msg);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (acceptsURL(url)) {
      Connection c = DELEGATE_DRIVER.connect(url.replace(URL_PREFIX, DELEGATE_DRIVER_PREFIX), info);
      if (c != null) {
        c = new LoggingConnection(c);
      } else {
        System.err.println("No connection from delegate driver!");
      }
      return c;
    } else {
      return null;
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url != null && url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return DELEGATE_DRIVER.getPropertyInfo(url, info);
  }

  @Override
  public boolean jdbcCompliant() {
    // should be compliant since just depending on mysql connector
    return DELEGATE_DRIVER.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return DELEGATE_DRIVER.getParentLogger();
  }

  /**
   * Connection implementation that will delegate to this classes {@link #log(String)} function as 
   * actions are invoked on it.
   */
  protected class LoggingConnection implements Connection {
    private final Connection delegateConnection;

    public LoggingConnection(Connection delegateConnection) throws SQLException {
      this.delegateConnection = delegateConnection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      log("unwarp:" + iface);
      return delegateConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      log("isWrapperFor:" + iface);
      return delegateConnection.isWrapperFor(iface);
    }

    @Override
    public Statement createStatement() throws SQLException {
      log("createStatement");
      return delegateConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
      log("prepareStatement:" + sql);
      return new LoggingPreparedStatement(delegateConnection.prepareStatement(sql));
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
      log("prepareCall:" + sql);
      return new LoggingCallableStatement(delegateConnection.prepareCall(sql));
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
      log("nativeSQL:" + sql);
      return delegateConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
      log("setAutoCommit:" + autoCommit);
      delegateConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
      log("getAutoCommit");
      return delegateConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
      log("commit");
      delegateConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
      log("rollback");
      delegateConnection.rollback();
    }

    @Override
    public void close() throws SQLException {
      log("close");
      delegateConnection.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
      log("isClosed");
      return delegateConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      log("getMetaData");
      return delegateConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
      log("setReadOnly:" + readOnly);
      delegateConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
      log("isReadOnly");
      return delegateConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
      log("setCatalog:" + catalog);
      delegateConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
      log("getCatalog");
      return delegateConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
      log("setTransactionIsolation:" + level);
      delegateConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
      log("getTransactionIsolation");
      return delegateConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      log("getWarnings");
      return delegateConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
      log("clearWarnings");
      delegateConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException {
      log("createStatement:" + resultSetType + ";" + resultSetConcurrency);
      return new LoggingStatement(delegateConnection.createStatement(resultSetType, 
                                                                     resultSetConcurrency));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
      log("prepareStatement:" + sql + ";" + resultSetType + ";" + resultSetConcurrency);
      return new LoggingPreparedStatement(delegateConnection.prepareStatement(sql, resultSetType, 
                                                                              resultSetConcurrency));
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
      log("prepareCall:" + sql + ";" + resultSetType + ";" + resultSetConcurrency);
      return new LoggingCallableStatement(delegateConnection.prepareCall(sql, resultSetType, 
                                                                         resultSetConcurrency));
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
      log("getTypeMap");
      return delegateConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      log("setTypeMap:" + map);
      delegateConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
      log("setHoldability:" + holdability);
      delegateConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
      log("getHoldability");
      return delegateConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
      log("setSavepoint");
      return delegateConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
      log("setSavepoint:" + name);
      return delegateConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
      log("rollback:" + savepoint);
      delegateConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      log("releaseSavepoint:" + savepoint);
      delegateConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
      log("createStatement:" + resultSetType + ";" + resultSetConcurrency + ";" + resultSetHoldability);
      return new LoggingStatement(delegateConnection.createStatement(resultSetType, 
                                                                     resultSetConcurrency, 
                                                                     resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
                                                  throws SQLException {
      log("prepareStatement:" + sql + ";" + resultSetType + ";" + resultSetConcurrency + ";" + resultSetHoldability);
      return new LoggingPreparedStatement(delegateConnection.prepareStatement(sql, resultSetType, 
                                                                              resultSetConcurrency, 
                                                                              resultSetHoldability));
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
      log("prepareCall:" + sql + ";" + resultSetType + ";" + resultSetConcurrency + ";" + resultSetHoldability);
      return new LoggingCallableStatement(delegateConnection.prepareCall(sql, resultSetType, 
                                                                         resultSetConcurrency, 
                                                                         resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
      log("prepareStatement:" + sql + ";" + autoGeneratedKeys);
      return new LoggingPreparedStatement(delegateConnection.prepareStatement(sql, autoGeneratedKeys));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
        throws SQLException {
      log("prepareStatement:" + sql + ";" + Arrays.toString(columnIndexes));
      return new LoggingPreparedStatement(delegateConnection.prepareStatement(sql, columnIndexes));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException {
      log("prepareStatement:" + sql + ";" + Arrays.toString(columnNames));
      return new LoggingPreparedStatement(delegateConnection.prepareStatement(sql, columnNames));
    }

    @Override
    public Clob createClob() throws SQLException {
      log("createClob");
      return delegateConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
      log("createBlob");
      return delegateConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
      log("createNClob");
      return delegateConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
      log("createSQLXML");
      return delegateConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
      log("isValid:" + timeout);
      return delegateConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
      log("setClientInfo:" + name + ";" + value);
      delegateConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
      log("setClientInfo:" + properties);
      delegateConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
      log("getClientInfo:" + name);
      return delegateConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
      log("getClientInfo");
      return delegateConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      log("createArrayOf:" + typeName + ";" + Arrays.deepToString(elements));
      return delegateConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      log("createStruct:" + typeName + ";" + Arrays.toString(attributes));
      return delegateConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
      log("setSchema");
      delegateConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
      log("getSchema");
      return delegateConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
      log("abort:" + (executor == null ? null : executor.getClass()));
      delegateConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      log("setNetworkTimeout:" + (executor == null ? null : executor.getClass()) + ";" + milliseconds);
      delegateConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
      log("getNetworkTimeout");
      return delegateConnection.getNetworkTimeout();
    }
  }
  
  /**
   * Implementation of {@link Statement} that logs all invocations before delegating to a provided 
   * implementation.
   */
  protected class LoggingStatement implements Statement {
    private final Statement s;

    public LoggingStatement(Statement s) {
      this.s = s;
    }
    
    protected String getStatementLogType() {
      return "Statement";
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      log(getStatementLogType() + ".isWrapperFor:" + iface);
      return s.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      log(getStatementLogType() + ".unwrap:" + iface);
      return s.unwrap(iface);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
      log(getStatementLogType() + ".addBatch");
      s.addBatch(sql);
    }

    @Override
    public void cancel() throws SQLException {
      log(getStatementLogType() + ".cancel");
      s.cancel();
    }

    @Override
    public void clearBatch() throws SQLException {
      log(getStatementLogType() + ".clearBatch");
      s.clearBatch();
    }

    @Override
    public void clearWarnings() throws SQLException {
      log(getStatementLogType() + ".clearWarnings");
      s.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
      log(getStatementLogType() + ".close");
      s.close();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
      log(getStatementLogType() + ".closeOnCompletion");
      s.closeOnCompletion();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
      log(getStatementLogType() + ".execute:" + sql);
      return s.execute(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      log(getStatementLogType() + ".execute:" + sql + "," + autoGeneratedKeys);
      return s.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      log(getStatementLogType() + ".execute:" + sql + "," + Arrays.toString(columnIndexes));
      return s.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
      log(getStatementLogType() + ".execute:" + sql + "," + Arrays.toString(columnNames));
      return s.execute(sql, columnNames);
    }

    @Override
    public int[] executeBatch() throws SQLException {
      log(getStatementLogType() + ".executeBatch");
      return s.executeBatch();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
      log(getStatementLogType() + ".executeQuery:" + sql);
      return s.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
      log(getStatementLogType() + ".executeUpdate:" + sql);
      return s.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      log(getStatementLogType() + ".executeUpdate:" + sql + "," + autoGeneratedKeys);
      return s.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      log(getStatementLogType() + ".executeUpdate:" + sql + "," + Arrays.toString(columnIndexes));
      return s.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      log(getStatementLogType() + ".executeUpdate:" + sql + "," + Arrays.toString(columnNames));
      return s.executeUpdate(sql, columnNames);
    }

    @Override
    public Connection getConnection() throws SQLException {
      log(getStatementLogType() + ".getConnection");
      return s.getConnection();
    }

    @Override
    public int getFetchDirection() throws SQLException {
      log(getStatementLogType() + ".getFetchDirection");
      return s.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
      log(getStatementLogType() + ".getFetchSize");
      return s.getFetchSize();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
      log(getStatementLogType() + ".getGeneratedKeys");
      return s.getGeneratedKeys();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
      log(getStatementLogType() + ".getMaxFieldSize");
      return s.getMaxFieldSize();
    }

    @Override
    public int getMaxRows() throws SQLException {
      log(getStatementLogType() + ".getMaxRows");
      return s.getMaxRows();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
      log(getStatementLogType() + ".getMoreResults");
      return s.getMoreResults();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
      log(getStatementLogType() + ".getMoreResults:" + current);
      return s.getMoreResults(current);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
      log(getStatementLogType() + ".getQueryTimeout");
      return s.getQueryTimeout();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
      log(getStatementLogType() + ".getResultSet");
      return s.getResultSet();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
      log(getStatementLogType() + ".getResultSetConcurrency");
      return s.getResultSetConcurrency();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
      log(getStatementLogType() + ".getResultSetHoldability");
      return s.getResultSetHoldability();
    }

    @Override
    public int getResultSetType() throws SQLException {
      log(getStatementLogType() + ".getResultSetType");
      return s.getResultSetType();
    }

    @Override
    public int getUpdateCount() throws SQLException {
      log(getStatementLogType() + ".getUpdateCount");
      return s.getUpdateCount();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      log(getStatementLogType() + ".getWarnings");
      return s.getWarnings();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
      log(getStatementLogType() + ".isCloseOnCompletion");
      return s.isCloseOnCompletion();
    }

    @Override
    public boolean isClosed() throws SQLException {
      log(getStatementLogType() + ".isClosed");
      return s.isClosed();
    }

    @Override
    public boolean isPoolable() throws SQLException {
      log(getStatementLogType() + ".isPoolable");
      return s.isPoolable();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
      log(getStatementLogType() + ".setCursorName:" + name);
      s.setCursorName(name);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
      log(getStatementLogType() + ".setEscapeProcessing:" + enable);
      s.setEscapeProcessing(enable);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
      log(getStatementLogType() + ".setFetchDirection:" + direction);
      s.setFetchDirection(direction);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
      log(getStatementLogType() + ".setFetchSize:" + rows);
      s.setFetchSize(rows);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
      log(getStatementLogType() + ".setMaxFieldSize:" + max);
      s.setMaxFieldSize(max);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
      log(getStatementLogType() + ".setMaxRows:" + max);
      s.setMaxRows(max);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
      log(getStatementLogType() + ".setPoolable:" + poolable);
      s.setPoolable(poolable);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
      log(getStatementLogType() + ".setQueryTimeout:" + seconds);
      s.setQueryTimeout(seconds);
    }
  }

  /**
   * Implementation of {@link PreparedStatement} that logs all invocations before delegating to a 
   * provided implementation.
   */
  protected class LoggingPreparedStatement extends LoggingStatement implements PreparedStatement {
    private final PreparedStatement ps;

    public LoggingPreparedStatement(PreparedStatement preparedStatement) {
      super(preparedStatement);
      
      ps = preparedStatement;
    }
    
    protected String getStatementLogType() {
      return "PreparedStatement";
    }

    @Override
    public void addBatch() throws SQLException {
      log(getStatementLogType() + ".addBatch");
      ps.addBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
      log(getStatementLogType() + ".clearParameters");
      ps.clearParameters();
    }

    @Override
    public boolean execute() throws SQLException {
      log(getStatementLogType() + ".execute");
      return ps.execute();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
      log(getStatementLogType() + ".executeQuery");
      return ps.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
      log(getStatementLogType() + ".executeUpdate");
      return ps.executeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      log(getStatementLogType() + ".getMetaData");
      return ps.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
      log(getStatementLogType() + ".getParameterMetaData");
      return ps.getParameterMetaData();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
      log(getStatementLogType() + ".setArray:" + parameterIndex);
      ps.setArray(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
      log(getStatementLogType() + ".setAsciiStream:" + parameterIndex);
      ps.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
      log(getStatementLogType() + ".setAsciiStream:" + parameterIndex + "," + length);
      ps.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
      log(getStatementLogType() + ".setAsciiStream:" + parameterIndex + "," + length);
      ps.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
      log(getStatementLogType() + ".setBigDecimal:" + parameterIndex + "," + x);
      ps.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
      log(getStatementLogType() + ".setBinaryStream:" + parameterIndex);
      ps.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
      log(getStatementLogType() + ".setBinaryStream:" + parameterIndex + "," + length);
      ps.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
      log(getStatementLogType() + ".setBinaryStream:" + parameterIndex + "," + length);
      ps.setBinaryStream(parameterIndex, x, length);
      
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
      log(getStatementLogType() + ".setBlob:" + parameterIndex);
      ps.setBlob(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
      log(getStatementLogType() + ".setBlob:" + parameterIndex);
      ps.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
      log(getStatementLogType() + ".setBlob:" + parameterIndex + "," + length);
      ps.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
      log(getStatementLogType() + ".setBoolean:" + parameterIndex + "," + x);
      ps.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
      log(getStatementLogType() + ".setByte:" + parameterIndex + "," + x);
      ps.setByte(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
      log(getStatementLogType() + ".setBytes:" + parameterIndex + "," + Arrays.toString(x));
      ps.setBytes(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      log(getStatementLogType() + ".setCharacterStream:" + parameterIndex);
      ps.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
      log(getStatementLogType() + ".setCharacterStream:" + parameterIndex + "," + length);
      ps.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
      log(getStatementLogType() + ".setCharacterStream:" + parameterIndex + "," + length);
      ps.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
      log(getStatementLogType() + ".setClob:" + parameterIndex);
      ps.setClob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
      log(getStatementLogType() + ".setClob:" + parameterIndex);
      ps.setClob(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
      log(getStatementLogType() + ".setClob:" + parameterIndex + "," + length);
      ps.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
      log(getStatementLogType() + ".setDate:" + parameterIndex + "," + x);
      ps.setDate(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".setDate:" + parameterIndex + "," + x + "," + cal);
      ps.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
      log(getStatementLogType() + ".setDouble:" + parameterIndex + "," + x);
      ps.setDouble(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
      log(getStatementLogType() + ".setFloat:" + parameterIndex + "," + x);
      ps.setFloat(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
      log(getStatementLogType() + ".setInt:" + parameterIndex + "," + x);
      ps.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
      log(getStatementLogType() + ".setLong:" + parameterIndex + "," + x);
      ps.setLong(parameterIndex, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
      log(getStatementLogType() + ".setNCharacterStream:" + parameterIndex);
      ps.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
      log(getStatementLogType() + ".setNCharacterStream:" + parameterIndex + "," + length);
      ps.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
      log(getStatementLogType() + ".setNClob:" + parameterIndex);
      ps.setNClob(parameterIndex, value);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      log(getStatementLogType() + ".setNClob:" + parameterIndex);
      ps.setNClob(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
      log(getStatementLogType() + ".setNClob:" + parameterIndex + "," + length);
      ps.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
      log(getStatementLogType() + ".setNString:" + parameterIndex + "," + value);
      ps.setNString(parameterIndex, value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
      log(getStatementLogType() + ".setNull:" + parameterIndex + "," + sqlType);
      ps.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
      log(getStatementLogType() + ".setNull:" + parameterIndex + "," + sqlType + "," + typeName);
      ps.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
      log(getStatementLogType() + ".setObject:" + parameterIndex);
      ps.setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
      log(getStatementLogType() + ".setObject:" + parameterIndex + "," + targetSqlType);
      ps.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
      log(getStatementLogType() + ".setObject:" + parameterIndex + "," + 
            targetSqlType + "," + scaleOrLength);
      ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
      log(getStatementLogType() + ".setRef:" + parameterIndex + "," + x);
      ps.setRef(parameterIndex, x);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
      log(getStatementLogType() + ".setRowId:" + parameterIndex + "," + x);
      ps.setRowId(parameterIndex, x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      log(getStatementLogType() + ".setSQLXML:" + parameterIndex);
      ps.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
      log(getStatementLogType() + ".setShort:" + parameterIndex + "," + x);
      ps.setShort(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
      log(getStatementLogType() + ".setString:" + parameterIndex + "," + x);
      ps.setString(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
      log(getStatementLogType() + ".setTime:" + parameterIndex);
      ps.setTime(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".setTime:" + parameterIndex + "," + cal);
      ps.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
      log(getStatementLogType() + ".setTimestamp:" + parameterIndex + "," + x);
      ps.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".setTimestamp:" + parameterIndex + "," + x + "," + cal);
      ps.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
      log(getStatementLogType() + ".setURL:" + parameterIndex + "," + x);
      ps.setURL(parameterIndex, x);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
      log(getStatementLogType() + ".setUnicodeStream:" + parameterIndex + "," + length);
      ps.setUnicodeStream(parameterIndex, x, length);
    }
  }

  /**
   * Implementation of {@link CallableStatement} that logs all invocations before delegating to a 
   * provided implementation.
   */
  protected class LoggingCallableStatement extends LoggingPreparedStatement implements CallableStatement {
    private final CallableStatement cs;

    public LoggingCallableStatement(CallableStatement prepareCall) {
      super(prepareCall);
      
      cs = prepareCall;
    }
    
    protected String getStatementLogType() {
      return "CallableStatement";
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getArray:" + parameterIndex);
      return cs.getArray(parameterIndex);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getArray:" + parameterName);
      return cs.getArray(parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getBigDecimal:" + parameterIndex);
      return cs.getBigDecimal(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getBigDecimal:" + parameterName);
      return cs.getBigDecimal(parameterName);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
      log(getStatementLogType() + ".getBigDecimal:" + parameterIndex + "," + scale);
      return cs.getBigDecimal(parameterIndex, scale);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getBlob:" + parameterIndex);
      return cs.getBlob(parameterIndex);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getBlob:" + parameterName);
      return cs.getBlob(parameterName);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getBoolean:" + parameterIndex);
      return cs.getBoolean(parameterIndex);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getBoolean:" + parameterName);
      return cs.getBoolean(parameterName);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getByte:" + parameterIndex);
      return cs.getByte(parameterIndex);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getByte:" + parameterName);
      return cs.getByte(parameterName);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getBytes:" + parameterIndex);
      return cs.getBytes(parameterIndex);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getBytes:" + parameterName);
      return cs.getBytes(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getCharacterStream:" + parameterIndex);
      return cs.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getCharacterStream:" + parameterName);
      return cs.getCharacterStream(parameterName);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getClob:" + parameterIndex);
      return cs.getClob(parameterIndex);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getClob:" + parameterName);
      return cs.getClob(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getDate:" + parameterIndex);
      return cs.getDate(parameterIndex);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getDate:" + parameterName);
      return cs.getDate(parameterName);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".getDate:" + parameterIndex + "," + cal);
      return cs.getDate(parameterIndex, cal);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".getDate:" + parameterName + "," + cal);
      return cs.getDate(parameterName, cal);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getDouble:" + parameterIndex);
      return cs.getDouble(parameterIndex);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getDouble:" + parameterName);
      return cs.getDouble(parameterName);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getFloat:" + parameterIndex);
      return cs.getFloat(parameterIndex);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getFloat:" + parameterName);
      return cs.getFloat(parameterName);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getInt:" + parameterIndex);
      return cs.getInt(parameterIndex);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getInt:" + parameterName);
      return cs.getInt(parameterName);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getLong:" + parameterIndex);
      return cs.getLong(parameterIndex);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getLong:" + parameterName);
      return cs.getLong(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getNCharacterStream:" + parameterIndex);
      return cs.getNCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getNCharacterStream:" + parameterName);
      return cs.getCharacterStream(parameterName);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getNClob:" + parameterIndex);
      return cs.getNClob(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
      log(getStatementLogType() + ".:getNClob" + parameterName);
      return cs.getNClob(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getNString:" + parameterIndex);
      return cs.getNString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getNString:" + parameterName);
      return cs.getNString(parameterName);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getObject:" + parameterIndex);
      return cs.getObject(parameterIndex);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getObject:" + parameterName);
      return cs.getObject(parameterName);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
      log(getStatementLogType() + ".getObject:" + parameterIndex + "," + map);
      return cs.getObject(parameterIndex, map);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
      log(getStatementLogType() + ".getObject:" + parameterName + "," + map);
      return cs.getObject(parameterName, map);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
      log(getStatementLogType() + ".getObject:" + parameterIndex + "," + type);
      return cs.getObject(parameterIndex, type);
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
      log(getStatementLogType() + ".getObject:" + parameterName + "," + type);
      return cs.getObject(parameterName, type);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getRef:" + parameterIndex);
      return cs.getRef(parameterIndex);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getRef:" + parameterName);
      return cs.getRef(parameterName);
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getRowId:" + parameterIndex);
      return cs.getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getRowId:" + parameterName);
      return cs.getRowId(parameterName);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getSQLXML:" + parameterIndex);
      return cs.getSQLXML(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getSQLXML:" + parameterName);
      return cs.getSQLXML(parameterName);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getShort:" + parameterIndex);
      return cs.getShort(parameterIndex);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getShort:" + parameterName);
      return cs.getShort(parameterName);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getString:" + parameterIndex);
      return cs.getString(parameterIndex);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getString:" + parameterName);
      return cs.getString(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getTime:" + parameterIndex);
      return cs.getTime(parameterIndex);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getTime:" + parameterName);
      return cs.getTime(parameterName);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".getTime:" + parameterIndex + "," + cal);
      return cs.getTime(parameterIndex, cal);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".:getTime" + parameterName + "," + cal);
      return cs.getTime(parameterName, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getTimestamp:" + parameterIndex);
      return cs.getTimestamp(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getTimestamp:" + parameterName);
      return cs.getTimestamp(parameterName);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".getTimestamp:" + parameterIndex + "," + cal);
      return cs.getTimestamp(parameterIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".getTimestamp:" + parameterName + "," + cal);
      return cs.getTimestamp(parameterName, cal);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
      log(getStatementLogType() + ".getURL:" + parameterIndex);
      return cs.getURL(parameterIndex);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
      log(getStatementLogType() + ".getURL:" + parameterName);
      return cs.getURL(parameterName);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
      log(getStatementLogType() + ".registerOutParameter:" + parameterIndex + "," + sqlType);
      cs.registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
      log(getStatementLogType() + ".registerOutParameter:" + parameterName + "," + sqlType);
      cs.registerOutParameter(parameterName, sqlType);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
      log(getStatementLogType() + ".registerOutParameter:" + parameterIndex + "," + sqlType + "," + scale);
      cs.registerOutParameter(parameterIndex, sqlType, scale);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
      log(getStatementLogType() + ".registerOutParameter:" + parameterIndex + "," + sqlType + "," + typeName);
      cs.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
      log(getStatementLogType() + ".registerOutParameter:" + parameterName + "," + sqlType + "," + scale);
      cs.registerOutParameter(parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
      log(getStatementLogType() + ".registerOutParameter:" + parameterName + "," + sqlType + "," + typeName);
      cs.registerOutParameter(parameterName, sqlType, typeName);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
      log(getStatementLogType() + ".setAsciiStream:" + parameterName);
      cs.setAsciiStream(parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
      log(getStatementLogType() + ".setAsciiStream:" + parameterName + "," + length);
      cs.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
      log(getStatementLogType() + ".setAsciiStream:" + parameterName + "," + length);
      cs.setAsciiStream(parameterName, x, length);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
      log(getStatementLogType() + ".setBigDecimal:" + parameterName);
      cs.setBigDecimal(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
      log(getStatementLogType() + ".setBinaryStream:" + parameterName);
      cs.setBinaryStream(parameterName, x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
      log(getStatementLogType() + ".setBinaryStream:" + parameterName + "," + length);
      cs.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
      log(getStatementLogType() + ".setBinaryStream:" + parameterName + "," + length);
      cs.setBinaryStream(parameterName, x, length);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
      log(getStatementLogType() + ".setBlob:" + parameterName);
      cs.setBlob(parameterName, x);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
      log(getStatementLogType() + ".setBlob:" + parameterName);
      cs.setBlob(parameterName, inputStream);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
      log(getStatementLogType() + ".setBlob:" + parameterName + "," + length);
      cs.setBlob(parameterName, inputStream, length);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
      log(getStatementLogType() + ".setBoolean:" + parameterName + "," + x);
      cs.setBoolean(parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
      log(getStatementLogType() + ".setByte:" + parameterName + "," + x);
      cs.setByte(parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
      log(getStatementLogType() + ".setBytes:" + parameterName + "," + Arrays.toString(x));
      cs.setBytes(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
      log(getStatementLogType() + ".setCharacterStream:" + parameterName);
      cs.setCharacterStream(parameterName, reader);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
      log(getStatementLogType() + ".setCharacterStream:" + parameterName + "," + length);
      cs.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
      log(getStatementLogType() + ".setCharacterStream:" + parameterName + "," + length);
      cs.setCharacterStream(parameterName, reader, length);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
      log(getStatementLogType() + ".setClob:" + parameterName);
      cs.setClob(parameterName, x);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
      log(getStatementLogType() + ".setClob:" + parameterName);
      cs.setClob(parameterName, reader);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
      log(getStatementLogType() + ".setClob:" + parameterName + "," + length);
      cs.setClob(parameterName, reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
      log(getStatementLogType() + ".setDate:" + parameterName);
      cs.setDate(parameterName, x);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".setDate:" + parameterName + "," + cal);
      cs.setDate(parameterName, x, cal);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
      log(getStatementLogType() + ".setDouble:" + parameterName + "," + x);
      cs.setDouble(parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
      log(getStatementLogType() + ".setFloat:" + parameterName + "," + x);
      cs.setFloat(parameterName, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
      log(getStatementLogType() + ".setInt:" + parameterName + "," + x);
      cs.setInt(parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
      log(getStatementLogType() + ".setLong:" + parameterName + "," + x);
      cs.setLong(parameterName, x);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
      log(getStatementLogType() + ".setNCharacterStream:" + parameterName);
      cs.setNCharacterStream(parameterName, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
      log(getStatementLogType() + ".setNCharacterStream:" + parameterName);
      cs.setNCharacterStream(parameterName, value, length);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
      log(getStatementLogType() + ".setNClob:" + parameterName);
      cs.setNClob(parameterName, value);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
      log(getStatementLogType() + ".setNClob:" + parameterName);
      cs.setNClob(parameterName, reader);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
      log(getStatementLogType() + ".setNClob:" + parameterName);
      cs.setNClob(parameterName, reader, length);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
      log(getStatementLogType() + ".setNString:" + parameterName + "," + value);
      cs.setNString(parameterName, value);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
      log(getStatementLogType() + ".setNull:" + parameterName + "," + sqlType);
      cs.setNull(parameterName, sqlType);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
      log(getStatementLogType() + ".setNull:" + parameterName + "," + sqlType + "," + typeName);
      cs.setNull(parameterName, sqlType, typeName);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
      log(getStatementLogType() + ".setObject:" + parameterName);
      cs.setObject(parameterName, x);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
      log(getStatementLogType() + ".setObject:" + parameterName + "," + targetSqlType);
      cs.setObject(parameterName, x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
      log(getStatementLogType() + ".setObject:" + parameterName + "," + targetSqlType + "," + scale);
      cs.setObject(parameterName, x, targetSqlType, scale);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
      log(getStatementLogType() + ".setRowId:" + parameterName);
      cs.setRowId(parameterName, x);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
      log(getStatementLogType() + ".setSQLXML:" + parameterName);
      cs.setSQLXML(parameterName, xmlObject);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
      log(getStatementLogType() + ".setShort:" + parameterName + "," + x);
      cs.setShort(parameterName, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
      log(getStatementLogType() + ".setString:" + parameterName + "," + x);
      cs.setString(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
      log(getStatementLogType() + ".setTime:" + parameterName + "," + x);
      cs.setTime(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".setTime:" + parameterName + "," + x + "," + cal);
      cs.setTime(parameterName, x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
      log(getStatementLogType() + ".setTimestamp:" + parameterName + "," + x);
      cs.setTimestamp(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
      log(getStatementLogType() + ".setTimestamp:" + parameterName + "," + x + "," + cal);
      cs.setTimestamp(parameterName, x, cal);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
      log(getStatementLogType() + ".setURL:" + parameterName + "," + val);
      cs.setURL(parameterName, val);
    }

    @Override
    public boolean wasNull() throws SQLException {
      log(getStatementLogType() + ".wasNull");
      return cs.wasNull();
    }
  }
}
