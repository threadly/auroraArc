package org.threadly.db;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

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

  public class LoggingConnection implements Connection {
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
      return delegateConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
      log("prepareCall:" + sql);
      return delegateConnection.prepareCall(sql);
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
      return delegateConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
      log("prepareStatement:" + sql + ";" + resultSetType + ";" + resultSetConcurrency);
      return delegateConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
      log("prepareCall:" + sql + ";" + resultSetType + ";" + resultSetConcurrency);
      return delegateConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
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
      return delegateConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
                                                  throws SQLException {
      log("prepareStatement:" + sql + ";" + resultSetType + ";" + resultSetConcurrency + ";" + resultSetHoldability);
      return delegateConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
      log("prepareCall:" + sql + ";" + resultSetType + ";" + resultSetConcurrency + ";" + resultSetHoldability);
      return delegateConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
      log("prepareStatement:" + sql + ";" + autoGeneratedKeys);
      return delegateConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
        throws SQLException {
      log("prepareStatement:" + sql + ";" + Arrays.toString(columnIndexes));
      return delegateConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException {
      log("prepareStatement:" + sql + ";" + Arrays.toString(columnNames));
      return delegateConnection.prepareStatement(sql, columnNames);
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
}
