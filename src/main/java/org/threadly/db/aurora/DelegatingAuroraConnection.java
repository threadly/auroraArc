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

import org.threadly.db.aurora.DelegatingAuroraConnection.ConnectionStateManager.ConnectionHolder;
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
  private final ConnectionStateManager connectionStateManager;
  private final ConnectionHolder[] connections;
  private final AuroraServer[] servers;
  private final Connection referenceConnection; // also stored in map, just used to global settings
  private final AuroraClusterMonitor clusterMonitor;
  private final AtomicBoolean closed;
  private volatile Pair<AuroraServer, ConnectionHolder> stickyConnection;

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
    
    if (urlArgs.contains("optimizedStateUpdates=true")) {
      connectionStateManager = new OptimizedConnectionStateManager();
    } else {
      connectionStateManager = new SafeConnectionStateManager();
    }
    
    Map<AuroraServer, ConnectionHolder> connections = new HashMap<>();
    ConnectionHolder firstConnectionHolder = null;
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
                        connectException != null ? null : connectionStateManager.wrapConnection(DelegateDriver.connect(s + urlArgs, info)));
      } catch (SQLException e) {
        connectException = new Pair<>(auroraServer, e);
      }
      if (connectException == null && firstConnectionHolder == null) {
        firstConnectionHolder = connections.get(auroraServer);
      }
    }
    this.connections = connections.values().toArray(new ConnectionHolder[connections.size()]);
    this.servers = connections.keySet().toArray(new AuroraServer[connections.size()]);
    referenceConnection = firstConnectionHolder.uncheckedState();
    clusterMonitor = AuroraClusterMonitor.getMonitor(connections.keySet());
    if (connectException != null) {
      clusterMonitor.expediteServerCheck(connectException.getLeft());
      throw connectException.getRight();
    }
    closed = new AtomicBoolean();
    stickyConnection = null;
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
          ch.uncheckedState().close();
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
    connectionStateManager.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() {
    return connectionStateManager.isReadOnly();
  }

  protected <T> T processOnDelegate(SQLOperation<T> action) throws SQLException {
    Pair<AuroraServer, ConnectionHolder> p = getDelegate();
    try {
      return action.run(p.getRight().verifiedState());
    } catch (SQLException e) {
      clusterMonitor.expediteServerCheck(p.getLeft());
      throw e;
    }
  }

  protected Pair<AuroraServer, ConnectionHolder> getDelegate() throws SQLException {
    // TODO - optimize without a lock, concern is non-auto commit in parallel returning two
    //        different connections.  In addition we use the `this` lock when setting the auto
    //        commit to `true` (ensuring the sticky connection is only cleared in a safe way)
    synchronized (this) {
      if (stickyConnection != null) {
        // at a point that state must be preserved
        return stickyConnection;
      }

      AuroraServer server;
      if (connectionStateManager.isReadOnly()) {
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
          Pair<AuroraServer, ConnectionHolder> result = new Pair<>(server, connections[i]);
          if (! connectionStateManager.isAutoCommit()) {
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
      connectionStateManager.setAutoCommit(autoCommit);
      if (autoCommit) {
        stickyConnection = null;
      }
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return connectionStateManager.isAutoCommit();
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
          ch.uncheckedState().setCatalog(catalog);
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
    connectionStateManager.setTransactionIsolationLevel(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return connectionStateManager.getTransactionIsolationLevel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    SQLWarning result = null;
    for (ConnectionHolder ch : connections) {
      SQLWarning conWarnings = ch.uncheckedState().getWarnings();
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
      ch.uncheckedState().clearWarnings();
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
        ch.uncheckedState().setTypeMap(map);
      }
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    synchronized (connections) {
      if (referenceConnection.getHoldability() != holdability) {
        for (ConnectionHolder ch : connections) {
          ch.uncheckedState().setHoldability(holdability);
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
      if (! ch.uncheckedState().isValid(remainingTimeout)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.uncheckedState().setClientInfo(name, value);
      }
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.uncheckedState().setClientInfo(properties);
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
          ch.uncheckedState().setSchema(schema);
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
        ch.uncheckedState().abort(executor);
      }
    }
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.uncheckedState().setNetworkTimeout(executor, milliseconds);
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
  
  /**
   * Class for maintaining state which is maintained and only updated lazily when the wrapped 
   * {@link ConnectionHolder#verifiedState()} is invoked.
   */
  protected static abstract class ConnectionStateManager {
    // state bellow is stored locally and set lazily on delegate connections
    protected volatile boolean readOnly;
    protected volatile boolean autoCommit;
    protected volatile int transactionIsolationLevel = Integer.MIN_VALUE; // used to indicate not initialized

    public void setReadOnly(boolean readOnly) {
      this.readOnly = readOnly;
    }
    
    public boolean isReadOnly() {
      return readOnly;
    }

    public void setAutoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
      return autoCommit;
    }

    public void setTransactionIsolationLevel(int level) {
      transactionIsolationLevel = level;
    }

    public int getTransactionIsolationLevel() {
      return transactionIsolationLevel;
    }
    
    public ConnectionHolder wrapConnection(Connection connection) throws SQLException {
      if (transactionIsolationLevel == Integer.MIN_VALUE) {
        // startup state from first connection we see
        readOnly = connection.isReadOnly();
        autoCommit = connection.getAutoCommit();
        transactionIsolationLevel = connection.getTransactionIsolation();
      }
      return makeConnectionHolder(connection);
    }
    
    protected abstract ConnectionHolder makeConnectionHolder(Connection connection);
    
    public static abstract class ConnectionHolder {
      protected final Connection connection;

      public ConnectionHolder(Connection connection) {
        this.connection = connection;
      }
      
      public Connection uncheckedState() {
        return connection;
      }

      public abstract Connection verifiedState() throws SQLException;
    }
  }
  
  /**
   * This state manager attempts to modify the delegated state update behavior as little as 
   * reasonably possible.  This means that even if the state has not updated, but which our 
   * connection has been invoked with an update to that state.  Or if it was updated to a different 
   * state, then updated back to the original state before use.  In both cases we will still forward 
   * these updates to the delegate connection.
   * <p>
   * This is currently considered the safer option because we modify the behavior less.  However 
   * the {@link OptimizedConnectionStateManager} provides an implementation to try and both reduce 
   * the forwarded updates, as well as overhead in tracking update practices.
   */
  protected static class SafeConnectionStateManager extends ConnectionStateManager {
    /* modification values are volatile, parallel missed updates would not be a problem since this is 
     * only to determine if a change has occurred since the last check (which would not get lost, also 
     * not to mention parallel updates occurring is almost impossibly low odds).
     *
     * These values can overflow without concern as well.
     * 
     * In concept, we could track the last known state and only update the delegate connection
     * if it has changed.  However in an attempt to modify behavior to the delegate connections as 
     * little as possible we will forward requests that we might have otherwise thought were useless.
     */
    private volatile int readOnlyModificationCount = 0;
    private volatile int autoCommitModificationCount = 0;
    private volatile int transactionIsolationLevelModificationCount = 0;

    @Override
    public void setReadOnly(boolean readOnly) {
      super.setReadOnly(readOnly);
      readOnlyModificationCount++;
    }

    public void setAutoCommit(boolean autoCommit) {
      super.setAutoCommit(autoCommit);
      autoCommitModificationCount++;
    }

    @Override
    public void setTransactionIsolationLevel(int level) {
      super.setTransactionIsolationLevel(level);
      transactionIsolationLevelModificationCount++;
    }

    @Override
    protected ConnectionHolder makeConnectionHolder(Connection connection) {
      return new SafeConnectionHolder(connection);
    }

    protected class SafeConnectionHolder extends ConnectionHolder {
      private int connectionReadOnlyModificationCount;
      private int connectionAutoCommitModificationCount;
      private int connectionIsolationLevelModificationCount;

      public SafeConnectionHolder(Connection connection) {
        super(connection);
        
        connectionReadOnlyModificationCount = readOnlyModificationCount;
        connectionAutoCommitModificationCount = autoCommitModificationCount;
        connectionIsolationLevelModificationCount = transactionIsolationLevelModificationCount;
      }

      public Connection verifiedState() throws SQLException {
        if (connectionReadOnlyModificationCount != readOnlyModificationCount) {
          connectionReadOnlyModificationCount = readOnlyModificationCount;
          connection.setReadOnly(readOnly);
        }
        if (connectionAutoCommitModificationCount != autoCommitModificationCount) {
          connectionAutoCommitModificationCount = autoCommitModificationCount;
          connection.setAutoCommit(autoCommit);
        }
        if (connectionIsolationLevelModificationCount != transactionIsolationLevelModificationCount) {
          connectionIsolationLevelModificationCount = transactionIsolationLevelModificationCount;
          connection.setTransactionIsolation(transactionIsolationLevel);
        }

        return connection;
      }
    }
  }
  
  /**
   * This state manager attempts to do the bare minimum to ensure that the connects state is 
   * communicated to the back end connection states.  It makes heavy assumptions that state updates 
   * can not occur outside of this state manager.
   */
  protected static class OptimizedConnectionStateManager extends ConnectionStateManager {
    @Override
    protected ConnectionHolder makeConnectionHolder(Connection connection) {
      return new OptimizedConnectionHolder(connection);
    }

    protected class OptimizedConnectionHolder extends ConnectionHolder {
      private boolean connectionReadOnly;
      private boolean connectionAutoCommit;
      private int connectionTransactionIsolationLevel;
  
      public OptimizedConnectionHolder(Connection connection) {
        super(connection);
        
        connectionReadOnly = readOnly;
        connectionAutoCommit = autoCommit;
        connectionTransactionIsolationLevel = transactionIsolationLevel;
      }

      @Override
      public Connection verifiedState() throws SQLException {
        if (connectionReadOnly != readOnly) {
          connectionReadOnly = readOnly;
          connection.setReadOnly(readOnly);
        }
        if (connectionAutoCommit != autoCommit) {
          connectionAutoCommit = autoCommit;
          connection.setAutoCommit(autoCommit);
        }
        if (connectionTransactionIsolationLevel != transactionIsolationLevel) {
          connectionTransactionIsolationLevel = transactionIsolationLevel;
          connection.setTransactionIsolation(transactionIsolationLevel);
        }
  
        return connection;
      }
    }
  }
}
