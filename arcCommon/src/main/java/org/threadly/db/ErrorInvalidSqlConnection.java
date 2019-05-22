package org.threadly.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link Connection} which is perpetually in a state of error.  Any operation on 
 * this connection will result in an exception being thrown.
 * <p>
 * The connection will appear invalid when checked by {@link #isValid(int)}, always returning 
 * {@code false}.
 * 
 * @since 0.10
 */
public class ErrorInvalidSqlConnection extends AbstractErrorSqlConnection {
  /**
   * Construct a new {@link ErrorInvalidSqlConnection}.
   * 
   * @param errorThrownListener Listener to be invoked when error is realized (ie thrown)
   * @param error Error to throw once Connection is attempted to be used
   */
  public ErrorInvalidSqlConnection(Runnable errorThrownListener, SQLException error) {
    super(errorThrownListener, error);
  }

  /**
   * Construct a new {@link ErrorInvalidSqlConnection}.
   * 
   * @param errorThrownListener Listener to be invoked when error is realized (ie thrown)
   * @param error Error to throw once Connection is attempted to be used
   */
  public ErrorInvalidSqlConnection(Runnable errorThrownListener, RuntimeException error) {
    super(errorThrownListener, error);
  }

  @Override
  public boolean isValid(int timeout) {
    return false;
  }
}
