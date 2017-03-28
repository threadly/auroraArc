package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DelegateDriver {
  protected static final String DRIVER_CONNECT_PREFIX = "jdbc:mysql://";
  protected static java.sql.Driver delegateDriver;  // not final so can be replaced in test

  static {
    try {
      // TODO - should we allow other delegate drivers?
      delegateDriver = new com.mysql.cj.jdbc.Driver();
    } catch (SQLException e) {
      // based off current state of mysql connector, this is not a possible exception anyways
      throw new RuntimeException(e);
    }
  }

  public static java.sql.Driver getDriver() {
    return delegateDriver;
  }

  public static Connection connect(String hostAndArgs, Properties info) throws SQLException {
    return delegateDriver.connect(DRIVER_CONNECT_PREFIX + hostAndArgs, info);
    //return java.sql.DriverManager.getConnection(hostAndArgs, info);
  }
}
