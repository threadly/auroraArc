package org.threadly.db.aurora;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import org.threadly.util.StringUtils;

/**
 * {@link DataSource} implementation which provides aurora {@link Driver} /
 * {@link DelegatingAuroraConnection} connections.
 * 
 * @since 0.5
 */
public final class AuroraDataSource implements DataSource, Referenceable, Serializable {
  private static final long serialVersionUID = -8925224249843386407L;
  private static final NonRegisteringDriver DRIVER = new NonRegisteringDriver();
  private static final int DEFAULT_PORT = 3306;
  
  private static String nullSafeRefAddrStringGet(String referenceName, Reference ref) {
    RefAddr refAddr = ref.get(referenceName);
    return (refAddr != null) ? (String) refAddr.getContent() : null;
  }
  
  protected transient PrintWriter logWriter;
  protected String databaseName;
  protected String hostName;
  protected int port;
  protected String user;
  protected String password;
  protected boolean explicitUrl;
  protected String url;
  
  /**
   * Default no-arg constructor for Serialization.
   */
  public AuroraDataSource() {
    // Default constructor for Serialization...
    logWriter = null;
    databaseName = null;
    hostName = null;
    port = DEFAULT_PORT;
    user = null;
    password = null;
    explicitUrl = false;
    url = null;
  }

  /**
   * Constructs and initializes driver properties from a JNDI reference.
   * 
   * @param ref The JNDI Reference that holds {@link RefAddr}s for all properties
   */
  protected AuroraDataSource(Reference ref) {
    this();  // get defaults
    
    String portNumberStr = nullSafeRefAddrStringGet("port", ref);
    if (! StringUtils.isNullOrEmpty(portNumberStr)) {
      int portNumber = Integer.parseInt(portNumberStr);
      setPort(portNumber);
    }
    
    String user = nullSafeRefAddrStringGet("user", ref);
    if (user != null) {
      setUsername(user);
    }
    
    String password = nullSafeRefAddrStringGet("password", ref);
    if (password != null) {
      setPassword(password);
    }
    
    String serverName = nullSafeRefAddrStringGet("serverName", ref);
    if (serverName != null) {
      setServerName(serverName);
    }
    
    String databaseName = nullSafeRefAddrStringGet("databaseName", ref);
    if (databaseName != null) {
      setDatabaseName(databaseName);
    }
    
    String explicitUrlAsString = nullSafeRefAddrStringGet("explicitUrl", ref);
    if (! StringUtils.isNullOrEmpty(explicitUrlAsString) && 
        Boolean.parseBoolean(explicitUrlAsString)) {
      setURL(nullSafeRefAddrStringGet("url", ref));
    }
  }
  
  @Override
  public Reference getReference() throws NamingException {
    String factoryName = AuroraDataSourceFactory.class.getName();
    Reference ref = new Reference(getClass().getName(), factoryName, null);
    ref.add(new StringRefAddr("user", this.user));
    ref.add(new StringRefAddr("password", this.password));
    ref.add(new StringRefAddr("serverName", getServerName()));
    ref.add(new StringRefAddr("port", Integer.toString(port)));
    ref.add(new StringRefAddr("databaseName", getDatabaseName()));
    ref.add(new StringRefAddr("explicitUrl", Boolean.toString(this.explicitUrl)));
    ref.add(new StringRefAddr("url", StringUtils.nullToEmpty(url)));
    
    return ref;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
  
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
  
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }
  
  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(this.user, this.password);
  }
  
  @Override
  public Connection getConnection(String userID, String pass) throws SQLException {
    Properties props = new Properties();
    
    if (userID != null) {
      props.setProperty("user", userID);
    }
    if (pass != null) {
      props.setProperty("password", pass);
    }
    
    return DRIVER.connect(getURL(), props);
  }
  
  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }
  
  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }
  
  @Override
  public void setLogWriter(PrintWriter output) throws SQLException {
    this.logWriter = output;
  }
  
  @Override
  public void setLoginTimeout(int timeout) throws SQLException {
    // Timeout not handled
  }
  
  /**
   * Sets the password to be used for new connections.
   * 
   * @param pass the password
   */
  public void setPassword(String pass) {
    this.password = pass;
  }
  
  /**
   * Sets the database port.
   * 
   * @param port the port to connect to
   */
  public void setPort(int port) {
    this.port = port;
  }
  
  /**
   * Returns the port number used for connecting to the database.
   * 
   * @return the port number used for connecting to the database
   */
  public int getPort() {
    return this.port;
  }
  
  /**
   * Sets the username to authenticate against the database with.
   * 
   * @param username the username to be paired with the password when connecting to database
   */
  public void setUsername(String username) {
    this.user = username;
  }
  
  /**
   * Returns the configured username for this connection.
   * 
   * @return the username for this connection
   */
  public String getUsername() {
    return this.user;
  }
  
  /**
   * Sets the database name.
   * 
   * @param dbName the name of the database
   */
  public void setDatabaseName(String dbName) {
    this.databaseName = dbName;
  }
  
  /**
   * Gets the name of the database.
   * 
   * @return the name of the database for this data source
   */
  public String getDatabaseName() {
    return StringUtils.nullToEmpty(this.databaseName);
  }
  
  /**
   * Sets the server name.
   * 
   * @param serverName the server name
   */
  public void setServerName(String serverName) {
    this.hostName = serverName;
  }
  
  /**
   * Returns the name of the database server.
   * 
   * @return the name of the database server
   */
  public String getServerName() {
    return StringUtils.nullToEmpty(this.hostName);
  }
  
  /**
   * This method is used by the app server to set the url string specified within the datasource 
   * deployment descriptor.  It is discovered using introspection and matches if property name in 
   * descriptor is "url".
   * 
   * @param url url to be used within driver.connect
   */
  public void setURL(String url) {
    this.url = url;
    this.explicitUrl = true;
  }
  
  /**
   * Returns the JDBC URL that will be used to create the database connection.  If set from 
   * {@link #setURL(String)} that exact value will be returned.  Otherwise a default generated URL 
   * will be provided from the other set fields.
   * 
   * @return the URL for this connection
   */
  public String getURL() {
    if (this.explicitUrl) {
      return this.url;
    } else {
      StringBuilder urlSb = new StringBuilder();
      urlSb.append(DelegatingAuroraConnection.URL_PREFIX)
           .append(getServerName()).append(":").append(port).append("/")
           .append(getDatabaseName());
      return urlSb.toString();
    }
  }
}
