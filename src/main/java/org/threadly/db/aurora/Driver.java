package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {
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
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public boolean jdbcCompliant() {
    // should be compliant since just depending on mysql connector
    return true;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO
    throw new UnsupportedOperationException();
  }
}
