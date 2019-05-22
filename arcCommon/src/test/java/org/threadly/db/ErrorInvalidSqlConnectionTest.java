package org.threadly.db;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

public class ErrorInvalidSqlConnectionTest extends AbstractErrorSqlConnectionTest {
  @Override
  protected AbstractErrorSqlConnection makeConnection(Runnable testListener, SQLException error) {
    return new ErrorInvalidSqlConnection(testListener, error);
  }

  @Test
  public void isValidTest() throws SQLException {
    assertFalse(connection.isValid(0));
    
    verifyAction(connection::error);
  }
}
