package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Driver for creating connections to a given delegate implementation.  This ultimately deals with 
 * creating a URL that is suited for that driver and then using it to establish the connection.
 */
public class DelegateDriver {
  protected static String driverConnectPrefix;
  protected static java.sql.Driver delegateDriver;  // not final so can be replaced in test

  static {
    try {
      setDelegateMysqlDriver(new com.mysql.cj.jdbc.Driver());
    } catch (SQLException e) {
      // based off current state of mysql connector, this is not a possible exception anyways
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Set the delegate driver and what the URL prefix should be.  This will only work for new 
   * connections, so should try to be done before any database initialization has been done.
   * 
   * @param urlPrefix Prefix expected by driver instance
   * @param driverInstance The instance of the driver which we should make connections from
   */
  public static void setDelegateDriver(String urlPrefix, java.sql.Driver driverInstance) {
    driverConnectPrefix = urlPrefix;
    delegateDriver = driverInstance;
  }
  
  /**
   * This is the same as {@link #setDelegateDriver(String, java.sql.Driver)} except that it 
   * defaults with a {@code "jdbc:mysql://"} connect prefix.
   * 
   * @param driverInstance The instance of the driver which we should make connections from
   */
  public static void setDelegateMysqlDriver(java.sql.Driver driverInstance) {
    setDelegateDriver("jdbc:mysql://", driverInstance);
  }

  /**
   * Get the delegated driver instance.
   * 
   * @return Instance of driver to delegate actual connections to
   */
  public static java.sql.Driver getDriver() {
    return delegateDriver;
  }

  /**
   * Connect using the delegate driver.
   * 
   * @param hostAndArgs Host and JDBC connect arguments WITHOUT the JDBC prefix
   * @param info Connection properties
   * @return A new connection or {@code null} if delegate is incorrectly configured
   * @throws SQLException Thrown if the delegate throws while connecting
   */
  public static Connection connect(String hostAndArgs, Properties info) throws SQLException {
    return delegateDriver.connect(driverConnectPrefix + hostAndArgs, info);
  }
}
