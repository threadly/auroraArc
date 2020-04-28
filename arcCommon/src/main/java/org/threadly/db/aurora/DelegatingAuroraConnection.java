package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.threadly.db.AbstractDelegatingConnection;
import org.threadly.db.ErrorValidSqlConnection;
import org.threadly.db.aurora.DelegatingAuroraConnection.ConnectionStateManager.ConnectionHolder;
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;

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
   * Used with {@link #setClientInfo(String, String)} and {@link #setClientInfo(Properties)} to 
   * control which delegate connection should be used.  Possible values are:
   * <ul>
   * <li>{@link #CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART} (default) - Will provide a replica server if likely safe
   * <li>{@link #CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY} - Will provide any server available
   * <li>{@link #CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED} - Will try to provide the master
   * <li>{@link #CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY} - Will only provide a healthy master
   * <li>{@link #CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED} - Will try to provide a random replica
   * <li>{@link #CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY} - Will only provide a healthy replica
   * </ul>
   */
  public static final String CLIENT_INFO_NAME_DELEGATE_CHOICE = "DelegateChoice";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will randomly choose either a master or replica server in a normalized distribution.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY = "Any";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will choose a replica server if the connection is read only and the driver believes 
   * it is safe to do so.  If a the preferred server type is unhealthy it will fallback to either 
   * the master or a replica if possible.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART = "Smart";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will try to always provide the master, unless it is unhealthy, in which case a 
   * replica will be used.  If no servers are healthy a {@link NoAuroraServerException} will be 
   * thrown.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED = "MasterPreferred";
  /**
   * @deprecated Use {@link CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED} instead
   */
  @Deprecated
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERED = "MasterPrefered";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will ONLY provide the master server, choosing to fail the request with a 
   * {@link NoAuroraServerException} if the master is unhealthy.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY = "MasterOnly";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will try to always provide a random replica, unless none are healthy, in which case 
   * the master will be used.  If no servers are healthy a {@link NoAuroraServerException} will be 
   * thrown.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED = "ReplicaPreferred";
  /**
   * @deprecated Use {@link CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED} instead.
   */
  @Deprecated
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERED = "ReplicaPrefered";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will ONLY provide a random replica server, choosing to fail the request with a 
   * {@link NoAuroraServerException} if there are no healthy replicas.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY = "ReplicaOnly";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will ONLY provide a random replica server from the first half of the available 
   * cluster.  If there is no available replica's the request will fail with a 
   * {@link NoAuroraServerException}.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY = "ReplicaPart1Only";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to 
   * {@link #setClientInfo(String, String)}.
   * <p>
   * This value will ONLY provide a random replica server from the second half of the available 
   * cluster.  This requires a minimum of {@code 2} healthy replica servers.  If one or no replicas 
   * are healthy a {@link NoAuroraServerException} will be thrown.
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_2_REPLICA_ONLY = "ReplicaPart2Only";
  /**
   * Possible value for {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} to
   * {@link #setClientInfo(String, String)}.
   *
   * <p> This value will try to pick a random server from the set that were marked "colocated" with
   * the local process, without regard to whether any particular server is master or replica. If
   * none are configured or none are available, behaves like
   * {@link CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY}.
   *
   * @see PROPERTY_COLOCATED_SERVERS
   */
  public static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_COLOCATED_PREFERRED = "ColocatedPreferred";
  protected static final String CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT = // may be set by providing null or empty
      CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART;

  /**
   * Identifier for a connection property that can be set to inform AuroraArc which delegate servers
   * are considered "colocated" with the local process.
   *
   * <p>If set, the value should be a non-negative integer indicating the number of servers which
   * are to be considered colocated. E.g., if a value of <em>n</em> is given, the servers at indices
   * 0 through <em>n</em>-1 will be considered colocated with the local process.
   */
  public static final String PROPERTY_COLOCATED_SERVERS = "auroraArcColocatedServers";

  /**
   * If this connection property is set to the value {@code true}, try to optimise state updates
   * (such as transaction status) sent to the servers.
   */
  public static final String PROPERTY_OPTIMIZED_STATE_UPDATES = "optimizedStateUpdates";

  protected static final Logger LOG = Logger.getLogger(AuroraClusterMonitor.class.getSimpleName());
  
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
  protected volatile String delegateChoice; // visible for testing
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

    String urlTail = url.substring(endDelim);

    boolean optimizedStateUpdates = false;
    int colocatedServers = 0;

    int urlArgsStart = url.indexOf('?', endDelim);
    if (-1 != urlArgsStart) {
      int argStart = urlArgsStart + 1;
      while (argStart < url.length()) {
        int argEnd = url.indexOf('&', argStart);
        if (-1 == argEnd) {
          argEnd = url.length();
        }

        int equal = url.indexOf('=', argStart);
        if (equal > 0 && equal < argEnd) {
          String argName = url.substring(argStart, equal);
          switch (argName) {
          case PROPERTY_OPTIMIZED_STATE_UPDATES:
            optimizedStateUpdates = Boolean.valueOf(url.substring(equal + 1, argEnd));
            break;

          case PROPERTY_COLOCATED_SERVERS:
            colocatedServers = Integer.parseInt(url.substring(equal + 1, argEnd));
            break;
          }
        }

        argStart = argEnd + 1;
      }
    }

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
          if (colocatedServers > i2) {
            colocatedServers--;
          }
          System.arraycopy(servers, i2 + 1, servers, i2, serverCount - i2);
          i2--;
        }
      }
    }

    if (optimizedStateUpdates) {
      connectionStateManager = new OptimizedConnectionStateManager();
    } else {
      connectionStateManager = new SafeConnectionStateManager();
    }
    
    delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT;
    
    ConnectionHolder firstConnectionHolder = null;
    this.servers = new AuroraServer[serverCount];
    this.connections = new ConnectionHolder[serverCount];
    // if we have an error connecting we still build the map (with empty values) so we can alert
    // other connections to check the status
    Pair<AuroraServer, SQLException> connectException = null;
    for (int i = 0; i < serverCount; i++) {
      this.servers[i] = new AuroraServer(servers[i], info, i < colocatedServers);
      try {
        connections[i] = connectionStateManager.wrapConnection(dDriver.connect(servers[i] + urlTail, info));
        if (firstConnectionHolder == null) {
          firstConnectionHolder = connections[i];
        }
      } catch (SQLException e) {
        LOG.log(Level.WARNING, "Delaying connect error for server: " + this.servers[i], e);
        if (connectException == null) {
          connectException = new Pair<>(this.servers[i], e);
        }
        connections[i] = 
            new ConnectionStateManager.UnverifiedConnectionHolder(
                new ErrorValidSqlConnection(this::closeSilently, e));
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Delaying connect error for server: " + this.servers[i], e);
        if (connectException == null) {
          connectException = new Pair<>(this.servers[i], new SQLException(e));
        }
        connections[i] = 
            new ConnectionStateManager.UnverifiedConnectionHolder(
                new ErrorValidSqlConnection(this::closeSilently, e));
      }
    }
    clusterMonitor = AuroraClusterMonitor.getMonitor(dDriver, this.servers);
    if (connectException != null) {
      clusterMonitor.expediteServerCheck(connectException.getLeft());
      if (firstConnectionHolder == null) {  // all connections are in error, throw now
        throw connectException.getRight();
      }
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
  
  protected void closeSilently() {
    try {
      close();
    } catch (SQLException e) {
      // ignored
    }
  }

  @Override
  public void close() throws SQLException {
    if (closed.compareAndSet(false, true)) {
      SQLException sqlError = null;
      RuntimeException runtimeError = null;
      synchronized (connections) {
        for (ConnectionHolder ch : connections) {
          try {
            ch.uncheckedState().close();
          } catch (SQLException e) {
            sqlError = e;
          } catch (RuntimeException e) {
            runtimeError = e;
          }
        }
      }
      if (sqlError != null) {
        throw sqlError;
      } else if (runtimeError != null) {
        throw runtimeError;
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
  public void setReadOnly(boolean readOnly) {
    connectionStateManager.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() {
    return connectionStateManager.isReadOnly();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new DelegatingAuroraDatabaseMetaData(referenceConnection.getMetaData());
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

      // We don't need to handle the misspelled "*Prefered" names here because setDelegateChoice()
      // handles that
      AuroraServer server;
      if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART.equals(delegateChoice)) {
        server = getAuroraServerSmart();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED.equals(delegateChoice)) {
        server = getAuroraServerMasterPreferred();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED.equals(delegateChoice)) {
        server = getAuroraServerReplicaPreferred();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY.equals(delegateChoice)) {
        server = getAuroraServerMasterOnly();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY.equals(delegateChoice)) {
        server = getAuroraServerReplicaOnly();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY.equals(delegateChoice)) {
        server = getAuroraServerReplicaHalf1Only();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_2_REPLICA_ONLY.equals(delegateChoice)) {
        server = getAuroraServerReplicaHalf2Only();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY.equals(delegateChoice)) {
        server = getAuroraServerAny();
      } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_COLOCATED_PREFERRED.equals(delegateChoice)) {
        server = getAuroraServerColocatedPreferred();
      } else {
        delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT; // reset to prevent further failures
        throw new IllegalStateException("Unknown delegate choice: " + delegateChoice);
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
      throw new IllegalStateException("Cluster monitor provided unknown server: " + server);
    }
  }

  protected AuroraServer getAuroraServerSmart() throws SQLException {
    if (connectionStateManager.isReadOnly()) {
      return getAuroraServerReplicaPreferred();
    } else {
      return getAuroraServerMasterPreferred();
    }
  }

  protected AuroraServer getAuroraServerMasterPreferred() throws SQLException {
    AuroraServer server = clusterMonitor.getCurrentMaster();
    if (server == null) {
      // we will _try_ to use a read only replica, since no master exists, lets hope this is a read
      server = clusterMonitor.getRandomReadReplica();
    }
    if (server == null) {
      throw new NoAuroraServerException("No healthy servers");
    }
    return server;
  }
  
  protected AuroraServer getAuroraServerMasterOnly() throws SQLException {
    AuroraServer server = clusterMonitor.getCurrentMaster();
    if (server == null) {
      throw new NoAuroraServerException("No healthy master server");
    }
    return server;
  }

  protected AuroraServer getAuroraServerReplicaPreferred() throws SQLException {
    AuroraServer server = clusterMonitor.getRandomReadReplica();
    if (server == null) {
      server = clusterMonitor.getCurrentMaster();
    }
    if (server == null) {
      throw new NoAuroraServerException("No healthy servers");
    }
    return server;
  }
  
  protected AuroraServer getAuroraServerReplicaOnly() throws SQLException {
    AuroraServer server = clusterMonitor.getRandomReadReplica();
    if (server == null) {
      throw new NoAuroraServerException("No healthy replica servers");
    }
    return server;
  }
  
  protected AuroraServer getAuroraServerReplicaHalf1Only() throws SQLException {
    int secondaryCount = clusterMonitor.getHealthyReplicaCount();
    AuroraServer server;
    if (secondaryCount > 2) {
      server = clusterMonitor.getReadReplica(ThreadLocalRandom.current().nextInt(secondaryCount / 2));
    } else {
      server = clusterMonitor.getReadReplica(0);
    }
    if (server == null) {
      throw new NoAuroraServerException("No healthy replica servers");
    }
    return server;
  }
  
  protected AuroraServer getAuroraServerReplicaHalf2Only() throws SQLException {
    int secondaryCount = clusterMonitor.getHealthyReplicaCount();
    AuroraServer server = null;
    if (secondaryCount > 2) {
      int halfStart = secondaryCount / 2;
      server = clusterMonitor.getReadReplica(halfStart + ThreadLocalRandom.current().nextInt(halfStart));
    } else {
      server = clusterMonitor.getReadReplica(1);
    }
    if (server == null) {
      if (secondaryCount == 0) {
        throw new NoAuroraServerException("No healthy replica servers");
      } else {
        throw new NoAuroraServerException("Only one healthy replica server");
      }
    }
    return server;
  }
  
  protected AuroraServer getAuroraServerAny() throws SQLException {
    int secondaryCount = clusterMonitor.getHealthyReplicaCount();
    AuroraServer server;
    if (secondaryCount == 0 || ThreadLocalRandom.current().nextInt(secondaryCount + 1) == 0) {
      server = clusterMonitor.getCurrentMaster();
      if (server == null) {
        server = clusterMonitor.getRandomReadReplica(); // retry with secondary
      }
    } else {
      server = clusterMonitor.getRandomReadReplica();
      if (server == null) {
        server = clusterMonitor.getCurrentMaster(); // retry with master
      }
    }
    if (server == null) {
      throw new NoAuroraServerException("No healthy servers");
    }
    return server;
  }

  protected AuroraServer getAuroraServerColocatedPreferred() throws SQLException {
    AuroraServer server = clusterMonitor.getRandomColocatedServer();
    if (null == server) {
      return getAuroraServerAny();
    } else {
      return server;
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
    if (timeout > 0) {
      long startTime = Clock.accurateForwardProgressingMillis();
      for (ConnectionHolder ch : connections) {
        int remainingTimeout = // seconds are gross
            timeout - (int)Math.floor((Clock.lastKnownForwardProgressingMillis() - startTime) / 1000.);
        if (remainingTimeout <= 0) {
          return false;
        } else if (! ch.uncheckedState().isValid(remainingTimeout)) {
          return false;
        }
      }
      return true;
    } else {
      for (ConnectionHolder ch : connections) {
        if (! ch.uncheckedState().isValid(timeout)) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (CLIENT_INFO_NAME_DELEGATE_CHOICE.equals(name)) {
      setDelegateChoice(value);
    } else {
      synchronized (connections) {
        for (ConnectionHolder ch : connections) {
          ch.uncheckedState().setClientInfo(name, value);
        }
      }
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    Object delegateChoice = properties.remove(CLIENT_INFO_NAME_DELEGATE_CHOICE);
    setDelegateChoice(delegateChoice == null ? null : delegateChoice.toString());

    synchronized (connections) {
      for (ConnectionHolder ch : connections) {
        ch.uncheckedState().setClientInfo(properties);
      }
    }
  }

  @SuppressWarnings("deprecation")
  protected void setDelegateChoice(String choice) {
    if (StringUtils.isNullOrEmpty(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED.equals(choice) ||
               CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERED.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERRED;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED.equals(choice) ||
               CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERED.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERRED;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_2_REPLICA_ONLY.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_2_REPLICA_ONLY;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY;
    } else if (CLIENT_INFO_VALUE_DELEGATE_CHOICE_COLOCATED_PREFERRED.equals(choice)) {
      delegateChoice = CLIENT_INFO_VALUE_DELEGATE_CHOICE_COLOCATED_PREFERRED;
    } else {
      throw new UnsupportedOperationException("Unknown delegate choice: " + choice);
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
    protected ConnectionHolder wrapConnection(Connection connection) throws SQLException {
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

    /**
     * Implementation of {@link ConnectionHolder} that will never do any state verifications.
     */
    public static class UnverifiedConnectionHolder extends ConnectionHolder {
      public UnverifiedConnectionHolder(Connection connection) {
        super(connection);
      }

      @Override
      public Connection verifiedState() {
        return connection;
      }
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
  
  /**
   * Extended implementation of {@link DelegatingDatabaseMetaData} so that implementation specific 
   * modifications can be made.  Specifically including the client info properties which this 
   * implementation supports.
   * 
   * @since 0.8
   */
  protected class DelegatingAuroraDatabaseMetaData extends DelegatingDatabaseMetaData {
    protected DelegatingAuroraDatabaseMetaData(DatabaseMetaData delegate) {
      super(delegate);
    }
    
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
      // TODO - provide ResultSet that defines our properties:
      /*
        rs.moveToInsertRow();
        rs.updateString("NAME", CLIENT_INFO_NAME_DELEGATE_CHOICE);
        rs.updateInt("MAX_LEN", );
        rs.updateString("DEFAULT_VALUE", CLIENT_INFO_VALUE_DELEGATE_CHOICE_DEFAULT);
        rs.updateString("DESCRIPTION", "Specify how the delegate connection should be chosen");
        rs.insertRow();
        rs.moveToCurrentRow();
      */

      return delegate.getClientInfoProperties();
    }
  }
  
  /**
   * Exception thrown when we there is no delegate aurora server that can be used.  This is most 
   * common when there is either no healthy servers in the cluster, or when the servers to be used 
   * is restricted by passing in {@link #CLIENT_INFO_NAME_DELEGATE_CHOICE} modifiers to 
   * {@link #setClientInfo(String, String)}.
   * 
   * @since 0.8
   */
  public static class NoAuroraServerException extends SQLException {
    private static final long serialVersionUID = -5962577973983028183L;

    protected NoAuroraServerException(String msg) {
      super(msg);
    }
  }
}
