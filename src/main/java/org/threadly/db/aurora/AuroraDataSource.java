package org.threadly.db.aurora;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

public final class AuroraDataSource implements DataSource, Referenceable, Serializable {
  
  private static final String AURORA_PREFIX = "jdbc:mysql:aurora:";
  
  private static final long serialVersionUID = 1L;
  
  /** The driver to create connections with */
  private static final NonRegisteringDriver driver;
  
  static {
    driver = new NonRegisteringDriver();
  }
  
  /** Log stream */
  protected transient PrintWriter logWriter = null;
  
  /** Database Name */
  protected String databaseName = null;
  
  /** Character Encoding */
  protected String encoding = null;
  
  /** Hostname */
  protected String hostName = null;
  
  /** Password */
  protected String password = null;
  
  /** The profileSQL property */
  protected String profileSQLString = "false";
  
  /** The JDBC URL */
  protected String url = null;
  
  /** User name */
  protected String user = null;
  
  /** Should we construct the URL, or has it been set explicitly */
  protected boolean explicitUrl = false;
  
  /** Port number */
  protected int port = 3306;
  
  /**
   * Default no-arg constructor for Serialization
   */
  public AuroraDataSource() {
    // Default constructor for Serialization...
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
  
  /**
   * Creates a new connection with the given username and passwordsetPropertiesViaRef
   * 
   * @param userID
   *          the user id to connect with
   * @param pass
   *          the password to connect with
   * 
   * @return a connection to the database
   * 
   * @throws SQLException
   *           if an error occurs
   */
  @Override
  public Connection getConnection(String userID, String pass) throws SQLException {
    Properties props = new Properties();
    
    if (userID != null) {
      props.setProperty("user", userID);
    }
    
    if (pass != null) {
      props.setProperty("password", pass);
    }
    
    return getConnection(props);
  }
  
  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.logWriter;
  }
  
  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }
  
  /**
   * Sets the log writer for this data source.
   * 
   * @see javax.sql.DataSource#setLogWriter(PrintWriter)
   */
  @Override
  public void setLogWriter(PrintWriter output) throws SQLException {
    this.logWriter = output;
  }
  
  @Override
  public void setLoginTimeout(int arg0) throws SQLException {
    // Timeout not handled
  }
  
  /**
   * Sets the password
   * 
   * @param pass
   *          the password
   */
  public void setPassword(String pass) {
    this.password = pass;
  }
  
  /**
   * Sets the database port.
   * 
   * @param p
   *          the port
   */
  public void setPort(int p) {
    this.port = p;
  }
  
  /**
   * Returns the port number
   * 
   * @return the port number
   */
  public int getPort() {
    return this.port;
  }
  
  /**
   * Sets the user ID.
   * 
   * @param userID
   *          the User ID
   */
  public void setUser(String userID) {
    this.user = userID;
  }
  
  /**
   * Returns the configured user for this connection
   * 
   * @return the user for this connection
   */
  public String getUser() {
    return this.user;
  }
  
  /**
   * Sets the database name.
   * 
   * @param dbName
   *          the name of the database
   */
  public void setDatabaseName(String dbName) {
    this.databaseName = dbName;
  }
  
  /**
   * Gets the name of the database
   * 
   * @return the name of the database for this data source
   */
  public String getDatabaseName() {
    return (this.databaseName != null) ? this.databaseName : "";
  }
  
  /**
   * Sets the server name.
   * 
   * @param serverName
   *          the server name
   */
  public void setServerName(String serverName) {
    this.hostName = serverName;
  }
  
  /**
   * Returns the name of the database server
   * 
   * @return the name of the database server
   */
  public String getServerName() {
    return (this.hostName != null) ? this.hostName : "";
  }
  
  /**
   * Sets the URL for this connection
   * 
   * @param url
   *          the URL for this connection
   */
  public void setURL(String url) {
    setUrl(url);
  }
  
  /**
   * Returns the URL for this connection
   * 
   * @return the URL for this connection
   */
  public String getURL() {
    return getUrl();
  }
  
  /**
   * This method is used by the app server to set the url string specified within the datasource deployment descriptor.
   * It is discovered using introspection and matches if property name in descriptor is "url".
   * 
   * @param url
   *          url to be used within driver.connect
   */
  public void setUrl(String url) {
    this.url = url;
    this.explicitUrl = true;
  }
  
  /**
   * Returns the JDBC URL that will be used to create the database connection.
   * 
   * @return the URL for this connection
   */
  public String getUrl() {
    if (!this.explicitUrl) {
      StringBuilder sbUrl = new StringBuilder(AURORA_PREFIX);
      sbUrl.append("//").append(getServerName()).append(":").append(getPort()).append("/").append(
                                                                                                  getDatabaseName());
      return sbUrl.toString();
    }
    return this.url;
  }
  
  /**
   * Creates a connection using the specified properties.
   * 
   * @param props
   *          the properties to connect with
   * 
   * @return a connection to the database
   * 
   * @throws SQLException
   *           if an error occurs
   */
  protected java.sql.Connection getConnection(Properties props) throws SQLException {
    final String jdbcUrlToUse = (!this.explicitUrl) ? getUrl() : this.url;
    return driver.connect(jdbcUrlToUse, props);
  }
  
  /**
   * Required method to support this class as a <CODE>Referenceable</CODE>.
   * 
   * @return a Reference to this data source
   * 
   * @throws NamingException
   *           if a JNDI error occurs
   */
  @Override
  public Reference getReference() throws NamingException {
    String factoryName = AuroraDataSourceFactory.class.getName();
    Reference ref = new Reference(getClass().getName(), factoryName, null);
    ref.add(new StringRefAddr("user", getUser()));
    ref.add(new StringRefAddr("password", this.password));
    ref.add(new StringRefAddr("serverName", getServerName()));
    ref.add(new StringRefAddr("port", String.valueOf(getPort())));
    ref.add(new StringRefAddr("databaseName", getDatabaseName()));
    ref.add(new StringRefAddr("url", getUrl()));
    ref.add(new StringRefAddr("explicitUrl", String.valueOf(this.explicitUrl)));
    
    return ref;
  }
  
  /**
   * Initializes driver properties that come from a JNDI reference (in the case of a javax.sql.DataSource bound into
   * some name service that doesn't handle Java objects directly).
   * 
   * @param ref
   *          The JNDI Reference that holds RefAddrs for all properties
   */
  public void setPropertiesViaRef(Reference ref) throws SQLException {
    // TODO implement it
  }
}
