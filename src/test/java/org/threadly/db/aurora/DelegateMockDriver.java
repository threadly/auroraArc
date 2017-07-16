package org.threadly.db.aurora;

import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.threadly.db.AbstractArcDriver;

public class DelegateMockDriver {
  public static final String MASTER_HOST = "masterHost";
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
      Connection mockConnection = mockConnections.get(host);
      if (mockConnection == null) {
        mockConnection = mock(Connection.class);
        
        // mock out the behavior for secondary check
        try {
          PreparedStatement mockStatement = mock(PreparedStatement.class);
          ResultSet mockResultSet = mock(ResultSet.class);
          when(mockStatement.executeQuery()).thenReturn(mockResultSet);
          when(mockResultSet.next()).thenReturn(true);
          if (MASTER_HOST.equalsIgnoreCase(host)) {
            when(mockResultSet.getString("Value")).thenReturn("OFF");
          } else {
            when(mockResultSet.getString("Value")).thenReturn("ON");
          }
          
          when(mockConnection.prepareStatement("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"))
            .thenReturn(mockStatement);
        } catch (SQLException e) {
          // not possible
        }
        
        mockConnections.put(host, mockConnection);
      }
      return mockConnection;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      int hostStartIndex = url.indexOf('/') + 2;
      int hostEndIndex = url.indexOf('/', hostStartIndex);
      int portIndex = url.indexOf(':', hostStartIndex);
      if (portIndex != -1 && portIndex < hostEndIndex) {
        hostEndIndex = portIndex;
      }
      return getConnectionForHost(url.substring(hostStartIndex, hostEndIndex));
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
