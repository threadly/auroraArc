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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.Clock;
import org.threadly.util.Pair;

public class DelegatingAuroraConnection implements Connection {
  public static final String URL_PREFIX = "jdbc:mysql:aurora://";

  public static boolean acceptsURL(String url) {
    return url != null && url.startsWith(DelegatingAuroraConnection.URL_PREFIX);
  }

  // TODO - Updates to connections are synchronized on connections
  //        We should figure out a better way to handle this, particularly for frequent operations
  //        like setting auto commit (maybe set locally and checked at delegate lookup time?)
  private final ConnectionHolder[] connections;
  private final AuroraServer[] servers;
  private final Connection referenceConnection; // also stored in map, just used to global settings
  private final AuroraClusterMonitor clusterMonitor;
  private final AtomicBoolean closed;
  private volatile Pair<AuroraServer, Connection> stickyConnection;
  // state bellow is stored locally and set lazily on delegate connections
  private final AtomicInteger modificationValue;  // can overflow
  private volatile boolean readOnly;
  private volatile boolean autoCommit;

  public DelegatingAuroraConnection(String url, Properties info) throws SQLException {
    int endDelim = url.indexOf('/', URL_PREFIX.length());
    // TODO - want to do more input url verification?
    if (endDelim < 0) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    String urlArgs = url.substring(endDelim);
    // TODO - lookup individual servers from a single cluster URL
    //        maybe derived from AuroraClusterMonitor to help ensure things remain consistent
    String[] servers = url.substring(URL_PREFIX.length(), endDelim).split(",");
    if (servers.length == 0) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    Map<AuroraServer, ConnectionHolder> connections = new HashMap<>();
    Connection firstConnection = null;
    // if we have an error connecting we still build the map (with empty values) so we can alert
    // other connections to check the status
    Pair<AuroraServer, SQLException> connectException = null;
    for (String s : servers) {
      AuroraServer auroraServer = new AuroraServer(s, info);
      if (connections.containsKey(auroraServer)) {
        continue;
      }
      try {
        connections.put(auroraServer,
                        connectException != null ? null : new ConnectionHolder(DelegateDriver.connect(s + urlArgs, info)));
      } catch (SQLException e) {
        connectException = new Pair<>(auroraServer, e);
      }
      if (connectException == null && firstConnection == null) {
        firstConnection = connections.get(auroraServer).connection;
      }
    }
    this.connections = connections.values().toArray(new ConnectionHolder[connections.size()]);
    this.servers = connections.keySet().toArray(new AuroraServer[connections.size()]);
    referenceConnection = firstConnection;
    clusterMonitor = AuroraClusterMonitor.getMonitor(connections.keySet());
    if (connectException != null) {
      clusterMonitor.expediteServerCheck(connectException.getLeft());
      throw connectException.getRight();
    }
    closed = new AtomicBoolean();
    stickyConnection = null;
    modificationValue = new AtomicInteger();
    readOnly = referenceConnection.isReadOnly();
    autoCommit = true;
  }
  
  @Override
  public String toString() {
    // TODO - I think we can produce a better string than this
    return DelegatingAuroraConnection.class.getSimpleName() + ":" + Arrays.toString(servers);
  }

  @Override
  public void close() throws SQLException {
    if (closed.compareAndSet(false, true)) {
      synchronized (connections) {
        for (ConnectionHolder ch : connections) {
          ch.connection.close();
        }
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // maintained locally and set lazily as delegate connections are returned
    if (this.readOnly != readOnly) {
      this.readOnly = readOnly;
      modificationValue.incrementAndGet();
    }
  }

  @Override
  public boolean isReadOnly() {
    return readOnly;
  }

  protected <T> T processOnDelegate(SQLOperation<T> action) throws SQLException {
    Pair<AuroraServer, Connection> p = getDelegate();
    try {
      return action.run(p.getRight());
    } catch (SQLException e) {
      clusterMonitor.expediteServerCheck(p.getLeft());
      throw e;
    }
  }

  protected Pair<AuroraServer, Connection> getDelegate() throws SQLException {
    // TODO - optimize without a lock, concern is non-auto commit in parallel returning two
    //        different connections.  In addition we use the `this` lock when setting the auto
    //        commit to `true` (ensuring the sticky connection is only cleared in a safe way)
    synchronized (this) {
      if (stickyConnection != null) {
        // at a point that state must be preserved
        return stickyConnection;
      }

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
      if (server == null) {
        throw new SQLException("No healthy servers");
      }
      // server should be guaranteed to be in map
      for (int i = 0; i < servers.length; i++) {
        if (servers[i].equals(server)) {
          Pair<AuroraServer, Connection> result = 
              new Pair<>(server, connections[i].updateConnectionStateAndReturn(this));
          if (! autoCommit) {
            stickyConnection = result;
          }
          return result;
        }
      }
      throw new IllegalStateException("Cluster monitor provided unknown server");
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // We are sort of a wrapper, but...it's complicated, so we pretend we are not
    try {
      return iface.cast(this);
    } catch (ClassCastException e) {
      throw new SQLException("Unable to unwrap to " + iface, e);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return referenceConnection.nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // maintained locally and set lazily as delegate connections are returned
    synchronized (this) {
      if (this.autoCommit != autoCommit) {
        this.autoCommit = autoCommit;
        if (autoCommit) {
          stickyConnection = null;
        }
        modificationValue.incrementAndGet();
      }
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return autoCommit;
  }

  @Override
  public void commit() throws SQLException {
    processOnDelegate((c) -> { c.commit(); return null; });
    // If understood correctly the sticky connection can now be reset to allow connection
    //       cycling even if autoCommit is disabled
    stickyConnection = null;
  }

  @Override
  public void rollback() throws SQLException {
    processOnDelegate((c) -> { c.rollback(); return null; });
    // If understood correctly the sticky connection can now be reset to allow connection
    //       cycling even if autoCommit is disabled
    stickyConnection = null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    processOnDelegate((c) -> { c.rollback(savepoint); return null; });
    // If understood correctly the sticky connection can now be reset to allow connection
    //       cycling even if autoCommit is disabled
    stickyConnection = null;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return processOnDelegate((c) -> c.getMetaData());
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    synchronized (connections) {
      if ((referenceConnection.getCatalog() == null && catalog != null) ||
          ! referenceConnection.getCatalog().equals(catalog)) {
        for (ConnectionHolder ch : connections) {
          ch.connection.setCatalog(catalog);
        }
      }
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    return referenceConnection.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    synchronized (connections) {
      if (referenceConnection.getTransactionIsolation() != level) {
        for (ConnectionHolder ch : connections) {
          ch.connection.setTransactionIsolation(level);
        }
      }
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return referenceConnection.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    SQLWarning result = null;
    for (ConnectionHolder ch : connections) {
      SQLWarning conWarnings = ch.connection.getWarnings();
      if (conWarnings != null) {
        if (result == null) {
          result = conWarnings;
        } else {
          result.setNextWarning(conWarnings);
        }
      }
    }
    return result;
  }

  @Override
  public void clearWarnings() throws SQLException {
    for (ConnectionHolder ch : connections) {
      ch.connection.clearWarnings();
    }
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return referenceConnection.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.connection.setTypeMap(map);
      }
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    synchronized (connections) {
      if (referenceConnection.getHoldability() != holdability) {
        for (ConnectionHolder ch : connections) {
          ch.connection.setHoldability(holdability);
        }
      }
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    return referenceConnection.getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return processOnDelegate((c) -> c.setSavepoint());
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return processOnDelegate((c) -> c.setSavepoint(name));
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    processOnDelegate((c) -> { c.releaseSavepoint(savepoint); return null; });
  }

  @Override
  public Statement createStatement() throws SQLException {
    return processOnDelegate((c) -> c.createStatement());
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return processOnDelegate((c) -> c.createStatement(resultSetType, resultSetConcurrency));
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws SQLException {
    return processOnDelegate((c) -> c.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return processOnDelegate((c) -> c.prepareStatement(sql));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency) throws SQLException {
    return processOnDelegate((c) -> c.prepareStatement(sql, resultSetType, resultSetConcurrency));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    return processOnDelegate((c) -> c.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return processOnDelegate((c) -> c.prepareCall(sql));
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency) throws SQLException {
    return processOnDelegate((c) -> c.prepareCall(sql, resultSetType, resultSetConcurrency));
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    return processOnDelegate((c) -> c.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return processOnDelegate((c) -> c.prepareStatement(sql, autoGeneratedKeys));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return processOnDelegate((c) -> c.prepareStatement(sql, columnIndexes));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return processOnDelegate((c) -> c.prepareStatement(sql, columnNames));
  }

  @Override
  public Clob createClob() throws SQLException {
    return referenceConnection.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return referenceConnection.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return referenceConnection.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return referenceConnection.createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    long startTime = timeout == 0 ? 0 : Clock.accurateForwardProgressingMillis();
    for (ConnectionHolder ch : connections) {
      int remainingTimeout;
      if (timeout == 0) {
        remainingTimeout = 0;
      } else {
        // seconds are gross
        remainingTimeout = timeout - (int)Math.floor((Clock.lastKnownForwardProgressingMillis() - startTime) / 1000.);
        if (remainingTimeout <= 0) {
          return false;
        }
      }
      if (! ch.connection.isValid(remainingTimeout)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.connection.setClientInfo(name, value);
      }
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.connection.setClientInfo(properties);
      }
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return referenceConnection.getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return referenceConnection.getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return referenceConnection.createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return referenceConnection.createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    synchronized (connections) {
      if ((referenceConnection.getSchema() == null && schema != null) ||
          ! referenceConnection.getSchema().equals(schema)) {
        for (ConnectionHolder ch : connections) {
          ch.connection.setSchema(schema);
        }
      }
    }
  }

  @Override
  public String getSchema() throws SQLException {
    return referenceConnection.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    synchronized (connections) {
      closed.set(true);
      for (ConnectionHolder ch : connections) {
        ch.connection.abort(executor);
      }
    }
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.connection.setNetworkTimeout(executor, milliseconds);
      }
    }
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return referenceConnection.getNetworkTimeout();
  }
  
  protected interface SQLOperation<T> {
    public T run(Connection connection) throws SQLException;
  }

  protected static class ConnectionHolder {
    protected final Connection connection;
    protected int modificationValue = 0;

    public ConnectionHolder(Connection connection) {
      this.connection = connection;
    }

    public Connection updateConnectionStateAndReturn(DelegatingAuroraConnection connectionState) throws SQLException {
      int expectedValue = connectionState.modificationValue.get();
      if (modificationValue != expectedValue) {
        if (connection.isReadOnly() != connectionState.readOnly) {
          connection.setReadOnly(connectionState.readOnly);
        }
        if (connection.getAutoCommit() != connectionState.autoCommit) {
          connection.setAutoCommit(connectionState.autoCommit);
        }
        modificationValue = expectedValue;
      }

      return connection;
    }
  }
}
