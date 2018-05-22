package org.threadly.db.aurora.mysql;

import java.sql.SQLException;

import org.threadly.db.aurora.DelegateDriver;

/**
 * Driver for creating connections to a given delegate implementation.  This ultimately deals with 
 * creating a URL that is suited for that driver and then using it to establish the connection.
 */
public final class MySqlDelegateDriver extends DelegateDriver {
  /**
   * Construct a new delegate driver for the mysql driver {@code com.mysql.cj.jdbc.Driver}.
   * 
   * @throws SQLException Thrown if the mysql driver throws an exception on construction
   */
  public MySqlDelegateDriver() throws SQLException {
    super("jdbc:mysql:aurora://", "jdbc:mysql://", new com.mysql.cj.jdbc.Driver());
  }
}
