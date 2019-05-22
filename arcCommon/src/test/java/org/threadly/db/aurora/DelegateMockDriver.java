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
import org.threadly.util.Pair;

public class DelegateMockDriver {
  public static final String MASTER_HOST = "masterHost";
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final Pair<String, DelegateAuroraDriver>[] ORIGINAL_DELEGATE_DRIVERS = 
    new Pair[DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS.length];

  static {
    for (int i = 0; i < ORIGINAL_DELEGATE_DRIVERS.length; i++) {
      ORIGINAL_DELEGATE_DRIVERS[i] = DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS[i];
    }
  }

  public static Pair<MockDriver, DelegateAuroraDriver> setupMockDriverAsDelegate() {
    MockDriver md = new MockDriver();
    DelegateAuroraDriver dd = new DelegateAuroraDriver("jdbc:aurora://", "jdbc:aurora://", md) {
      @Override
      public String getDriverName() {
        return "MockDriver";
      }
      
      @Override
      public boolean isMasterServer(AuroraServer server, Connection serverConnection) throws SQLException {
        // default logic matches mysql
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
                throw new IllegalDriverStateException("Unknown db state, may require library upgrade: " + readOnlyStr);
              }
            } else {
              throw new IllegalDriverStateException("No result looking up db state, likely not connected to Aurora database");
            }
          }
        }
      }
    };
    for (int i = 0; i < DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS.length; i++) {
      DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS[i] = 
          new Pair<String, DelegateAuroraDriver>(DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS[i].getLeft(), dd);
    }
    return new Pair<>(md, dd);
  }

  public static void resetDriver() {
    for (int i = 0; i < ORIGINAL_DELEGATE_DRIVERS.length; i++) {
      DelegateAuroraDriver.DEFAULT_IMPLEMENTATIONS[i] = ORIGINAL_DELEGATE_DRIVERS[i];
    }
  }

  public static class MockDriver extends AbstractArcDriver {
    private final Map<String, Connection> mockConnections = new HashMap<>();

    public Connection getConnectionForHost(String host) {
      Connection mockConnection = mockConnections.get(host);
      if (mockConnection == null) {
        mockConnection = mock(Connection.class);
        
        try {
          // mock out the behavior for secondary check on mysql
          when(mockConnection.isValid(anyInt())).thenReturn(true);
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
          
          // mock out the behavior for secondary check on psql
          /*PreparedStatement preapredStatement = mock(PreparedStatement.class);
          ResultSet resultSet = mock(ResultSet.class);
          if (! host.equalsIgnoreCase(MASTER_HOST)) {
            when(resultSet.next()).thenReturn(true, true, false);
            when(resultSet.getString("server_id")).thenReturn(MASTER_HOST, host);
            when(resultSet.getString("session_id")).thenReturn(UUID.randomUUID().toString());
          } else {
            when(resultSet.next()).thenReturn(true, false);
            when(resultSet.getString("server_id")).thenReturn(MASTER_HOST);
            when(resultSet.getString("session_id")).thenReturn("MASTER_SESSION_ID");
          }
          
          when(mockConnection.prepareStatement("SELECT server_id, session_id FROM aurora_replica_status();"))
            .thenReturn(preapredStatement);*/
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
