package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.threadly.db.AbstractArcDriver;
/**
 * Threadly's AuroraArc non-registering Driver.  This Driver will create multiple connections for 
 * each returned connection it provides.  Using these connections to monitor the aurora state, and 
 * to distribute queries to multiple aurora servers when possible.
 * <p>
 * Note: This driver is NOT registering connection to the connection manager, and is typically used 
 * by a JNDI compatible application server.  Please see {@link Driver} for more complete javadocs, 
 * since that is what most people will find useful.
 */
public class NonRegisteringDriver extends AbstractArcDriver {
  /**
   * Construct a new driver.
   */
  public NonRegisteringDriver() {
    // Required for Class.forName().newInstance()
  }
  
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (DelegatingAuroraConnection.acceptsURL(url)) {
      return new DelegatingAuroraConnection(url, info);
    } else {
      // JDBC spec specifies that if unhandled URL type at this point is provided, null should be returned
      return null;
    }
  }
  
  @Override
  public boolean acceptsURL(String url) {
    return DelegatingAuroraConnection.acceptsURL(url);
  }
  
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return DelegateDriver.driverForArcUrl(url).getDriver().getPropertyInfo(url, info);
  }
  
  @Override
  public boolean jdbcCompliant() {
    return DelegateDriver.getAnyDelegateDriver().getDriver().jdbcCompliant();
  }
  
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return DelegateDriver.getAnyDelegateDriver().getDriver().getParentLogger();
  }
}
