package org.threadly.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link Connection} which is perpetually in a state of error.  Any operation on 
 * this connection will result in an exception being thrown.
 * <p>
 * The connection will appear valid (from {@link #isValid(int)} and non-closed UNTIL the exception 
 * is thrown.  After that point the connection will appear as if it was closed.
 * 
 * @since 0.10
 */
public class ErrorValidSqlConnection extends AbstractErrorSqlConnection {
  /**
   * Construct a new {@link ErrorValidSqlConnection}.
   * 
   * @param errorThrownListener Listener to be invoked when error is realized (ie thrown)
   * @param error Error to throw once Connection is attempted to be used
   */
  public ErrorValidSqlConnection(Runnable errorThrownListener, SQLException error) {
    super(errorThrownListener, error);
  }

  /**
   * Construct a new {@link ErrorValidSqlConnection}.
   * 
   * @param errorThrownListener Listener to be invoked when error is realized (ie thrown)
   * @param error Error to throw once Connection is attempted to be used
   */
  public ErrorValidSqlConnection(Runnable errorThrownListener, RuntimeException error) {
    super(errorThrownListener, error);
  }

  @Override
  public boolean isValid(int timeout) {
    return ! isClosed();
  }
}
