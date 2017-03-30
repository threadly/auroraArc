package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.threadly.db.AbstractArcDriver;

public class Driver extends AbstractArcDriver {
  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Another way to register the driver.  This is more convenient than `Class.forName(String)` as 
   * no exceptions need to be handled (instead just relying on the compile time dependency).
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
