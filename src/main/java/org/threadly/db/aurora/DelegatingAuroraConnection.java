package org.threadly.db.aurora;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class DelegatingAuroraConnection implements Connection {
  public static final String URL_PREFIX = "jdbc:mysql:aurora://";
  public static final java.sql.Driver DELEGATE_DRIVER;
  
  static {
    try {
      DELEGATE_DRIVER = new com.mysql.cj.jdbc.Driver();
    } catch (SQLException e) {
      // based off current state of mysql connector, this is not a possible exception anyways
      throw new RuntimeException(e);
    }
  }
  
  public static boolean acceptsURL(String url) {
    return url != null && url.startsWith(DelegatingAuroraConnection.URL_PREFIX);
  }
  
  private final Map<AuroraServer, Connection> connections;
  private final AuroraClusterMonitor clusterMonitor;
  private final AtomicBoolean closed;
  private boolean readOnly;
  
  public DelegatingAuroraConnection(String url, Properties info) throws SQLException {
    int endDelim = url.indexOf('/', URL_PREFIX.length());
    // TODO - want to do more input url verification?
    if (endDelim < 0) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    String urlArgs = url.substring(endDelim);
    String[] servers = url.substring(URL_PREFIX.length(), endDelim).split(",");
    if (servers.length == 0) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    List<AuroraServer> clusterServers = new ArrayList<>(servers.length);
    connections = new HashMap<>();
    for (String s : servers) {
      AuroraServer auroraServer = new AuroraServer(s);
      if (connections.containsKey(auroraServer)) {
        continue;
      }
      String mysqlConnectUrl = "jdbc:mysql://" + s + urlArgs;
      connections.put(auroraServer, DELEGATE_DRIVER.connect(mysqlConnectUrl, info));
    }
    clusterMonitor = AuroraClusterMonitor.getMonitor(clusterServers);
    closed = new AtomicBoolean();
    readOnly = false;
  }

  @Override
  public void close() throws SQLException {
    if (closed.compareAndSet(false, true)) {
      for (Connection c : connections.values()) {
        c.close();
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    // TODO - anything need to occur on state change?
    this.readOnly = readOnly;
  }

  @Override
  public boolean isReadOnly() {
    return readOnly;
  }
  
  protected Connection getDelegate() {
    // TODO - if can not switch delegate, return existing
    AuroraServer server;
    if (readOnly) {
      server = clusterMonitor.getRandomReadReplica();
      if (server == null) {
        server = clusterMonitor.getCurrentMaster();
      }
    } else {
      server = clusterMonitor.getCurrentMaster();
      if (server == null) {
        // we will _try_ to use a read only replica, since no master exists, lets hope this is a read
        server = clusterMonitor.getRandomReadReplica();
      }
    }
    Connection result = connections.get(server);  // server should be guaranteed to be in map
    // TODO - set result in case we need to sticky to connection
    return result;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return getDelegate().unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return getDelegate().isWrapperFor(iface);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return getDelegate().nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // TODO - state need to be stored locally, or set on all connections?
    getDelegate().setAutoCommit(autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return getDelegate().getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    // TODO - allow delegate connection to change now?
    getDelegate().commit();
  }

  @Override
  public void rollback() throws SQLException {
    // TODO - allow delegate connection to change now?
    getDelegate().rollback();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return getDelegate().getMetaData();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // TODO - state need to be stored locally, or set on all connections?
    getDelegate().setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    return getDelegate().getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    getDelegate().setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return getDelegate().getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return getDelegate().getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    getDelegate().clearWarnings();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return getDelegate().getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    // TODO - state need to be stored locally?
    getDelegate().setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    // TODO - state need to be stored locally?
    getDelegate().setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    return getDelegate().getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return getDelegate().setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return getDelegate().setSavepoint(name);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO - allow delegate connection to change now?
    getDelegate().rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    getDelegate().releaseSavepoint(savepoint);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return getDelegate().createStatement();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return getDelegate().createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws SQLException {
    return getDelegate().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return getDelegate().prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, 
                                            int resultSetConcurrency) throws SQLException {
    return getDelegate().prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    return getDelegate().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return getDelegate().prepareCall(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, 
                                       int resultSetConcurrency) throws SQLException {
    return getDelegate().prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    return getDelegate().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return getDelegate().prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return getDelegate().prepareStatement(sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return getDelegate().prepareStatement(sql, columnNames);
  }

  @Override
  public Clob createClob() throws SQLException {
    return getDelegate().createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return getDelegate().createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return getDelegate().createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return getDelegate().createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return getDelegate().isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // TODO - state need to be stored locally, or set on all connections?
    getDelegate().setClientInfo(name, value);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // TODO - state need to be stored locally, or set on all connections?
    getDelegate().setClientInfo(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return getDelegate().getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return getDelegate().getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return getDelegate().createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return getDelegate().createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    // TODO - state need to be stored locally, or set on all connections?
    getDelegate().setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return getDelegate().getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    getDelegate().abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // TODO - state need to be stored locally, or set on all connections?
    getDelegate().setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return getDelegate().getNetworkTimeout();
  }
}
