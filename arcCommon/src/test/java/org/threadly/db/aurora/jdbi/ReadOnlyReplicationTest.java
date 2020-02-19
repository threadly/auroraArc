package org.threadly.db.aurora.jdbi;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.db.LoggingDriver;
import org.threadly.db.aurora.DelegateAuroraDriver;
import org.threadly.db.aurora.DelegateMockDriver;
import org.threadly.db.aurora.Driver;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;
import org.threadly.db.aurora.DelegatingAuroraConnection;

@SuppressWarnings("unused")
public class ReadOnlyReplicationTest {
  private MockDriver mockDriver;
  private Connection slaveMockConnection;
  private Connection masterMockConnection;
  private Jdbi dbi;
  
  @BeforeClass
  public static void setupClass() {
    Driver.registerDriver();
  }

  @Before
  public void setup() throws SQLException {
    mockDriver = DelegateMockDriver.setupMockDriverAsDelegate().getLeft();
    String slaveHost = "slaveHost";
    slaveMockConnection = mockDriver.getConnectionForHost(slaveHost);
    masterMockConnection = mockDriver.getConnectionForHost(DelegateMockDriver.MASTER_HOST);

    dbi = Jdbi.create(DelegateAuroraDriver.getAnyDelegateDriver().getArcPrefix() +  
                        slaveHost + "," + DelegateMockDriver.MASTER_HOST + 
                        "/auroraArc?useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC",
                      "auroraArc", "");
    dbi.installPlugin(new SqlObjectPlugin());
  }

  @After
  public void cleanup() {
    DelegateMockDriver.resetDriver();
    mockDriver = null;
    slaveMockConnection = null;
    masterMockConnection = null;
    dbi = null;
  }
  
  protected static PreparedStatement prepareMockConnectionForQuery(Connection connection) throws SQLException {
    // if ran on the master connection this will likely result in a NullPointerException
    when(connection.createStatement(anyInt(), anyInt())).thenReturn(mock(Statement.class));
    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    when(mockPreparedStatement.getResultSet()).thenReturn(mock(ResultSet.class));
    when(connection.prepareStatement("SELECT test FROM fake_table LIMIT 1"))
      .thenReturn(mockPreparedStatement);
    when(connection.prepareStatement(eq("SELECT test FROM fake_table LIMIT 1"), anyInt(), anyInt()))
      .thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.execute()).thenReturn(true);
    return mockPreparedStatement;
  }
  
  @Test
  public void verifyMasterServerChoice() throws SQLException {
    TestDAO dao = dbi.onDemand(TestDAO.class);
    // if ran on the master connection this will likely result in a NullPointerException
    PreparedStatement mockPreparedStatement = prepareMockConnectionForQuery(masterMockConnection);
    
    dao.masterRead();
    
    verify(mockPreparedStatement).execute();
  }
  
  @Test
  public void verifyReadOnlyServerChoice() throws SQLException {
    TestDAO dao = dbi.onDemand(TestDAO.class);
    // if ran on the master connection this will likely result in a NullPointerException
    PreparedStatement mockPreparedStatement = prepareMockConnectionForQuery(slaveMockConnection);
    
    dao.replicaRead();
    
    verify(mockPreparedStatement).execute();
  }
  
  public interface TestDAO {
    @ReadOnly
    @SqlQuery("SELECT test FROM fake_table LIMIT 1")
    public String replicaRead();
    
    @SqlQuery("SELECT test FROM fake_table LIMIT 1")
    public String masterRead();
  }
}
