package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.threadly.db.AbstractDelegatingConnection;
import org.threadly.db.aurora.DelegatingAuroraConnection.ConnectionStateManager.ConnectionHolder;
import org.threadly.util.Clock;
import org.threadly.util.Pair;

/**
 * Implementation of {@link Connection} which under the hood delegates to one of several 
 * connections to an Aurora server.  This uses the {@link AuroraClusterMonitor} to monitor the 
 * provided cluster, and intelligently choose which connection to delegate to.
 * <p>
 * From an external perspective this is just a single connection doing some magic to fairly 
 * distribute load if possible, and to handle fail over conditions with minimal impact.
 */
public class DelegatingAuroraConnection extends AbstractDelegatingConnection implements Connection {
  /**
   * Check if a given URL is accepted by this connection.
   * 
   * @param url URL to test
   * @return {@code true} if this connection is able to handled the provided url
   */
  public static boolean acceptsURL(String url) {
    return DelegateAuroraDriver.driverForArcUrl(url) != null;
  }

  /* Updates for frequently changed state values is managed by the `ConnectionStateManager`. 
   * Updates which need to applied immediately, or which are unlikely to be performance concerns 
   * are in contrast updated while synchronizing on the `connections` array and applied to all 
   * in the calling thread.  If any of these synchronized states show to be performance sensitive 
   * areas we may move them into the `ConnectionStateManager`, but we want to avoid doing that 
   * unnecessarily so that it does not become burdened with too much state tracking.
   */
  protected final ConnectionStateManager connectionStateManager;
  protected final AuroraServer[] servers;
  private final String driverName;
  private final ConnectionHolder[] connections;
  private final Connection referenceConnection; // also stored in map, just used to global settings
  private final AuroraClusterMonitor clusterMonitor;
  private final AtomicBoolean closed;
  private volatile Pair<AuroraServer, ConnectionHolder> stickyConnection;

  /**
   * Construct a new connection with the provided url and info.  Aurora servers should be 
   * delineated by {@code ','}.  Ultimately this will depend on {@link DelegateAuroraDriver} to delegate 
   * to an implementation based off the provided {@code url}.
   * 
   * @param url Servers and connect properties to connect to
   * @param info Info properties
   * @throws SQLException Thrown if issue in establishing delegate connections
   */
  public DelegatingAuroraConnection(String url, Properties info) throws SQLException {
    DelegateAuroraDriver dDriver = DelegateAuroraDriver.driverForArcUrl(url);
    if (dDriver == null) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    driverName = dDriver.getDriverName();
    
    int endDelim = url.indexOf('/', dDriver.getArcPrefix().length());
    if (endDelim < 0) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    String urlArgs = url.substring(endDelim);
    // TODO - lookup individual servers from a single cluster URL
    //        maybe derived from AuroraClusterMonitor to help ensure things remain consistent
    String[] servers = url.substring(dDriver.getArcPrefix().length(), endDelim).split(",");
    if (servers.length == 0) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }
    // de-duplicate servers if needed
    int serverCount = servers.length; // can't reference .length again, may contain duplicate records
    for (int i1 = 0; i1 < serverCount; i1++) {
      for (int i2 = i1 + 1; i2 < serverCount; i2++) {
        if (servers[i1].equals(servers[i2])) {
          serverCount--;
          System.arraycopy(servers, i2 + 1, servers, i2, serverCount - i2);
          i2--;
        }
      }
    }
    
    if (urlArgs.contains("optimizedStateUpdates=true")) {
      connectionStateManager = new OptimizedConnectionStateManager();
    } else {
      connectionStateManager = new SafeConnectionStateManager();
    }
    
    ConnectionHolder firstConnectionHolder = null;
    this.servers = new AuroraServer[serverCount];
    this.connections = new ConnectionHolder[serverCount];
    // if we have an error connecting we still build the map (with empty values) so we can alert
    // other connections to check the status
    Pair<AuroraServer, SQLException> connectException = null;
    for (int i = 0; i < serverCount; i++) {
      this.servers[i] = new AuroraServer(servers[i], info);
      if (connectException == null) {
        try {
          connections[i] = connectionStateManager.wrapConnection(dDriver.connect(servers[i] + urlArgs, info));
          if (firstConnectionHolder == null) {
            firstConnectionHolder = connections[i];
          }
        } catch (SQLException e) {
          connectException = new Pair<>(this.servers[i], e);
        }
      }
    }
    clusterMonitor = AuroraClusterMonitor.getMonitor(dDriver, this.servers);
    if (connectException != null) {
      clusterMonitor.expediteServerCheck(connectException.getLeft());
      throw connectException.getRight();
    }
    referenceConnection = firstConnectionHolder.uncheckedState();
    closed = new AtomicBoolean();
    stickyConnection = null;
  }
  
  @Override
  protected String getDriverName() {
    return driverName;
  }

  @Override
  protected void resetStickyConnection() {
    stickyConnection = null;
  }
  
  @Override
  protected Connection getReferenceConnection() {
    return referenceConnection;
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
  public boolean isClosed() throws SQLException {
    if (closed.get()) {
      return true;
    } else {
      // TODO - would be nice to find a way to be push notified of closed events rather than lazily checking
      for (ConnectionHolder ch : connections) {
        if (ch.uncheckedState().isClosed()) {
          // TODO - do we want to ensure other connections are closed?
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    connectionStateManager.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() {
    return connectionStateManager.isReadOnly();
  }

  @Override
  protected <R> R processOnDelegate(SQLOperation<Connection, R> action) throws SQLException {
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
  
  /**
   * Class for maintaining state which is maintained and only updated lazily when the wrapped 
   * {@link ConnectionHolder#verifiedState()} is invoked.
   */
  protected abstract static class ConnectionStateManager {
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
    
    /**
     * Wrap the connection in a holder that will then be able to be kept in reference to the state 
     * that is modified on this manager.
     * 
     * @param connection Connection to be wrapped / updated
     * @return Holder specific to the connection provided
     * @throws SQLException Thrown if delegate connection throws while initializing the state
     */
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
    
    /**
     * Holder of a connection.  The connection can be retrieved from this holder.  At the time of 
     * requesting the connection if a consistent state is important {@link #verifiedState()} 
     * should be used.  Otherwise {@link #uncheckedState()} is a faster way to get to the internal 
     * connection reference.
     */
    public abstract static class ConnectionHolder {
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

    @Override
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

    /**
     * Connection holder which updates {@link #verifiedState()} in such a way that minimizes 
     * interactions from this delegating driver.
     */
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

      @Override
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

    /**
     * Connection holder which updates the state in {@link #verifiedState()} in such a way that 
     * minimizes processing and possible updates to the retained connection.  This should be safe 
     * but because it modifies behavior further the possibility for unexpected interactions are 
     * higher.
     */
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
