package org.threadly.db.aurora.psql;

import org.threadly.db.aurora.DelegateDriver;

/**
 * Driver for creating connections to a given delegate implementation.  This ultimately deals with 
 * creating a URL that is suited for that driver and then using it to establish the connection.
 */
public class PsqlDelegateDriver extends DelegateDriver {
  /**
   * Construct a new delegate driver for the postgresql driver {@code org.postgresql.Driver}.
   */
  public PsqlDelegateDriver() {
    super("jdbc:postgresql:aurora://", "jdbc:postgresql://", new org.postgresql.Driver());
  }
}
