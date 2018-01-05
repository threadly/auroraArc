package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.threadly.db.AbstractArcDriver;

/**
 * Threadly's AuroraArc NonRegisteringDriver. This Driver will create multiple connections for each returned connection
 * it provides. Using these connections to monitor the aurora state, and to distribute queries to multiple aurora
 * servers when possible.
 * <p>
 * This is different from most SQL drivers, in that there is shared state between all returned connections. This shared
 * state allows things like fail conditions to be communicated quickly, allowing for intelligence of how to mitigate
 * problems, and use potential secondary / slave servers as soon as possible.
 * <p>
 * In general the user does not need to concern themselves with this benefit. Just be aware that multiple connections
 * will be established for every connection returned by this Driver.
 * <p>
 * Possible URL configuration options:
 * <ul>
 * <li>{@code "optimizedStateUpdates=true"} - Experimental internal code that can provide performance gains
 * </ul>
 */
public class NonRegisteringDriver extends AbstractArcDriver {
  
  /**
   * Another way to register the driver. This is more convenient than `Class.forName(String)` as no exceptions need to
   * be handled (instead just relying on the compile time dependency).
   */
  public static void registerDriver() {
    // Nothing needed, just a nicer way to initialize the static registration compared to Class.forName.
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
    return DelegateDriver.getDriver().getPropertyInfo(url, info);
  }
  
  @Override
  public boolean jdbcCompliant() {
    // should be compliant since just depending on mysql connector
    return DelegateDriver.getDriver().jdbcCompliant();
  }
  
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return DelegateDriver.getDriver().getParentLogger();
  }
}
