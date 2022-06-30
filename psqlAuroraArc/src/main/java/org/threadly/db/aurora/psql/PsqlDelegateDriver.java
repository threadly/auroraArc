package org.threadly.db.aurora.psql;

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
public class PsqlDelegateDriver extends DelegateAuroraDriver {

  private static final int DEFAULT_PORT = 5432;

  /**
   * Construct a new delegate driver for the postgresql driver {@code org.postgresql.Driver}.
   */
  public PsqlDelegateDriver() {
    this(new org.postgresql.Driver());
  }
  
  /**
   * Construct a new delegate driver for the postgresql driver {@code org.postgresql.Driver}.
   * 
   * @param driver Delegate driver to use
   */
  public PsqlDelegateDriver(java.sql.Driver driver) {
    super("jdbc:postgresql:aurora://", "jdbc:postgresql://", driver);
  }
  
  @Override
  public String getDriverName() {
    return "auroraArc-psql";
  }

  @Override
  public int getDefaultPort() {
    return DEFAULT_PORT;
  }

  @Override
  public boolean isMasterServer(AuroraServer server, Connection connection) throws SQLException {
    int serverIdDelim = server.getHost().indexOf('.');
    if (serverIdDelim < 0) {
      throw new IllegalStateException("Invalid host: " + server.getHost());
    }
    String serverId = server.getHost().substring(0, serverIdDelim);
    try (PreparedStatement ps = 
           connection.prepareStatement("SELECT server_id, session_id FROM aurora_replica_status();")) {
      try (ResultSet results = ps.executeQuery()) {
        int count = 0;
        while (results.next()) {
          count++;
          if (serverId.equalsIgnoreCase(results.getString("server_id"))) {
            return "MASTER_SESSION_ID".equalsIgnoreCase(results.getString("session_id"));
          }
        }
        throw new IllegalDriverStateException("Could not find server '" + serverId + "' in cluster of " + count);
      }
    }
  }
}
