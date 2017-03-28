package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.threadly.db.AbstractArcDriver;

public class DelegateMockDriver {
  private static final java.sql.Driver ORIGINAL_DELEGATE_DRIVER = DelegateDriver.delegateDriver;

  public static MockDriver setupMockDriverAsDelegate() {
    MockDriver md = new MockDriver();
    DelegateDriver.delegateDriver = md;
    return md;
  }

  public static void resetDriver() {
    DelegateDriver.delegateDriver = ORIGINAL_DELEGATE_DRIVER;
  }

  public static class MockDriver extends AbstractArcDriver {
    private final Map<String, Connection> mockConnections = new HashMap<>();

    public Connection getConnectionForHost(String host) {
      return mockConnections.get(host);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      int hostStart = url.indexOf('/') + 2;
      String host = url.substring(hostStart, url.indexOf('/', hostStart));

      Connection mockConnection = mockConnections.get(host);
      if (mockConnection == null) {
        // TODO - mock out
        mockConnections.put(host, mockConnection);
      }
      return mockConnection;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
      return true;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }
  }
}
