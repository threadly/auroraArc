package org.threadly.db;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

public class ErrorValidSqlConnectionTest extends AbstractErrorSqlConnectionTest {
  @Override
  protected AbstractErrorSqlConnection makeConnection(Runnable testListener, SQLException error) {
    return new ErrorValidSqlConnection(testListener, error);
  }

  @Test
  public void isValidTest() throws SQLException {
    assertTrue(connection.isValid(0));
    
    verifyAction(connection::error);
    
    assertFalse(connection.isValid(0));
    assertTrue(connection.isClosed());
  }
}
