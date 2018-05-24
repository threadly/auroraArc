package org.threadly.db.aurora.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.threadly.db.aurora.AuroraServer;
import org.threadly.db.aurora.DelegateAuroraDriver;

/**
 * Driver for creating connections to a given delegate implementation.  This ultimately deals with 
 * creating a URL that is suited for that driver and then using it to establish the connection.
 */
public final class MySqlDelegateDriver extends DelegateAuroraDriver {
  /**
   * Construct a new delegate driver for the mysql driver {@code com.mysql.cj.jdbc.Driver}.
   * 
   * @throws SQLException Thrown if the mysql driver throws an exception on construction
   */
  public MySqlDelegateDriver() throws SQLException {
    this(new com.mysql.cj.jdbc.Driver());
  }

  /**
   * Construct a new delegate driver for the mysql driver {@code com.mysql.cj.jdbc.Driver}.
   * 
   * @param delegateDriver Driver to use
   */
  public MySqlDelegateDriver(java.sql.Driver delegateDriver) {
    super("jdbc:mysql:aurora://", "jdbc:mysql://", delegateDriver);
  }

  @Override
  public boolean isMasterServer(AuroraServer server, 
                                Connection serverConnection) throws IllegalDriverStateException, SQLException {
    try (PreparedStatement ps =
           serverConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';")) {
      try (ResultSet results = ps.executeQuery()) {
        if (results.next()) {
          // unless exactly "OFF" database will be considered read only
          String readOnlyStr = results.getString("Value");
          if (readOnlyStr.equals("OFF")) {
            return true;
          } else if (readOnlyStr.equals("ON")) {
            return false;
          } else {
            throw new IllegalDriverStateException(
                          "Unknown db state, may require library upgrade: " + readOnlyStr);
          }
        } else {
          throw new IllegalDriverStateException(
                        "No result looking up db state, likely not connected to Aurora database");
        }
      }
    }
  }
}
