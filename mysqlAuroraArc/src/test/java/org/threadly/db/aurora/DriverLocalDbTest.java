package org.threadly.db.aurora;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.result.ResultIterator;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.FetchSize;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;
import org.jdbi.v3.sqlobject.transaction.Transactional;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.threadly.concurrent.UnfairExecutor;
import org.threadly.db.LoggingDriver;
import org.threadly.db.aurora.Driver;
import org.threadly.db.aurora.AuroraClusterMonitor.AuroraServersKey;
import org.threadly.db.aurora.AuroraClusterMonitor.ClusterChecker;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;
import org.threadly.util.StackSuppressedRuntimeException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) // tests prefixed `a|z[0-9]_` to set order where it matters
public class DriverLocalDbTest {
  private static final boolean LOGGING_DRIVER = false;
  private static final boolean OPTIMIZED_DRIVER = true;
  protected static Jdbi DBI;

  @BeforeClass
  public static void setupClass() {
    if (LOGGING_DRIVER) {
      LoggingDriver.registerDriver();
    } else {
      Driver.registerDriver();
    }
    DBI = Jdbi.create("jdbc:mysql:" + (LOGGING_DRIVER ? "logging" : "aurora") + "://" +  
                        "127.0.0.1:3306,127.0.0.2:3306,127.0.0.3:3306,127.0.0.4:3306" + 
                        "/auroraArc?" + 
                        "useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false" + 
                        (OPTIMIZED_DRIVER ? "&optimizedStateUpdates=true" : ""),
                      "auroraArc", "");
    DBI.installPlugin(new SqlObjectPlugin());
  }
  
  protected Handle h;
  protected JdbiDao dao;

  @Before
  public void setup() {
    h = DBI.open();
    dao = h.attach(JdbiDao.class);
  }

  @After
  public void cleanup() throws SQLException {
    dao.getHandle().close();
    dao = null;
    h.close();
    h = null;
  }

  @Test
  public void a0_setup() throws InterruptedException {
    if (LOGGING_DRIVER) {
      return; // too much
    }
    UnfairExecutor executor = new UnfairExecutor(31);
    dao.deleteRecords();
    for (int i = 0; i < 1_000; i++) {
      executor.execute(() -> dao.insertRecord(StringUtils.makeRandomString(10)));
    }
    executor.shutdown();
    executor.awaitTermination();
  }

  @Test
  public void a1_insertRecordSmart() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART);
    dao.insertRecord(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordAny() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY);
    dao.insertRecord(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordMasterPrefered() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERED);
    dao.insertRecord(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordMasterOnly() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY);
    dao.insertRecord(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordSlavePrefered() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERED);
    dao.insertRecord(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordSlaveOnlyFail() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY);
    try {
      dao.insertRecord(StringUtils.makeRandomString(5));
      fail("Exception should have thrown");
    } catch (Exception e) {
      assertTrue(ExceptionUtils.hasCauseOfType(e, DelegatingAuroraConnection.NoAuroraServerException.class));
    }
  }

  @Test
  public void a1_insertRecordAndReturnId() {
    int id = dao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
    assertTrue(id > 1);
  }

  @Test
  public void a1_insertRecordInterfaceTransactionAndCount() {
    int expectedCount = dao.recordCount() + 1;
    int count = dao.insertRecordAndReturnCount(StringUtils.makeRandomString(5));
    assertEquals(expectedCount, count);
    assertEquals(expectedCount, dao.recordCount());
  }

  @Test
  public void a2_transactionInsertAndLookup() {
    dao.inTransaction((txDao) -> {
      txDao.lookupRecord(1);
      txDao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      return txDao.recordCount();
    });
  }

  @Test
  public void a3_lookupSingleRecordSmart() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_SMART);
    Pair<Long, String> p = dao.lookupRecord(1);
    assertNotNull(p);
  }

  @Test
  public void a3_lookupSingleRecordAny() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY);
    Pair<Long, String> p = dao.lookupRecord(1);
    assertNotNull(p);
  }

  @Test
  public void a3_lookupSingleRecordMasterPrefered() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_PREFERED);
    Pair<Long, String> p = dao.lookupRecord(1);
    assertNotNull(p);
  }

  @Test
  public void a3_lookupSingleRecordMasterOnly() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_MASTER_ONLY);
    Pair<Long, String> p = dao.lookupRecord(1);
    assertNotNull(p);
  }

  @Test
  public void a3_lookupSingleRecordSlavePrefered() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_PREFERED);
    Pair<Long, String> p = dao.lookupRecord(1);
    assertNotNull(p);
  }

  @Test
  public void a3_lookupSingleRecordSlaveOnlyFail() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_ANY_REPLICA_ONLY);
    try {
      dao.lookupRecord(1);
      fail("Exception should have thrown");
    } catch (Exception e) {
      assertTrue(ExceptionUtils.hasCauseOfType(e, DelegatingAuroraConnection.NoAuroraServerException.class));
    }
  }

  @Test
  public void a3_lookupSingleRecordFirstHalfSlaveOnlyFail() throws SQLClientInfoException {
    h.getConnection().setClientInfo(DelegatingAuroraConnection.CLIENT_INFO_NAME_DELEGATE_CHOICE, 
                                    DelegatingAuroraConnection.CLIENT_INFO_VALUE_DELEGATE_CHOICE_HALF_1_REPLICA_ONLY);
    try {
      dao.lookupRecord(1);
      fail("Exception should have thrown");
    } catch (Exception e) {
      assertTrue(ExceptionUtils.hasCauseOfType(e, DelegatingAuroraConnection.NoAuroraServerException.class));
    }
  }

  @Test
  public void lookupMissingRecord() {
    Pair<Long, String> p = dao.lookupRecord(-1);
    assertNull(p);
  }

  @Test (expected = StackSuppressedRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownBeforeAnyAction() {
    dao.inTransaction((txDao) -> {
      throw new StackSuppressedRuntimeException();
    });
  }

  @Test (expected = StackSuppressedRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterLookup() {
    dao.inTransaction((txDao) -> {
      txDao.lookupRecord(1);
      throw new StackSuppressedRuntimeException();
    });
  }

  @Test (expected = StackSuppressedRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterDone() {
    dao.inTransaction((txDao) -> {
      txDao.lookupRecord(1);
      txDao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      throw new StackSuppressedRuntimeException();
    });
  }
  
  @Test
  public void connectErrorDelayedTest() throws SQLException {
    AuroraServer[] servers = new AuroraServer[] { new AuroraServer("127.0.0.1:3306", new Properties()), 
                                                  new AuroraServer("127.0.0.2:6603", new Properties()) };
    // put in monitor so we don't fail trying to establish cluster monitor
    AuroraClusterMonitor.MONITORS.put(new AuroraServersKey(servers), 
                                      new AuroraClusterMonitor(mock(ClusterChecker.class)));
    
    Jdbi dbi = Jdbi.create("jdbc:mysql:aurora://127.0.0.1:3306,127.0.0.2:6603/auroraArc?" + 
                             "useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                           "auroraArc", "");
    
    try (Handle h = dbi.open()) {
      assertTrue(h.getConnection().isValid(0));
    }
  }

  @Test
  public void z_lookupRecordsPaged() throws InterruptedException {
    int expectedCount = Math.min(dao.recordCount(), 10_000);
    int count = 0;
    ResultIterator<?> it = dao.lookupAllRecords();
    while (it.hasNext()) {
      count++;
      it.next(); // ignore value
    }
    assertEquals(expectedCount, count);
  }

  @Test
  public void z_lookupRecordsCollection() {
    dao.lookupRecordsCreatedBefore(new Timestamp(Clock.lastKnownTimeMillis()));
  }

  @RegisterRowMapper(RecordPairMapper.class)
  public interface JdbiDao extends Transactional<JdbiDao> {
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public void insertRecord(@Bind("record") String record);

    @SqlUpdate("DELETE FROM records WHERE id != 1")
    public void deleteRecords();

    @GetGeneratedKeys
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public int insertRecordAndReturnId(@Bind("record") String record);

    @Transaction (value = TransactionIsolationLevel.SERIALIZABLE)
    default int insertRecordAndReturnCount(String record) {
      insertRecordAndReturnId(record);
      return recordCount();
    }

    @SqlQuery("SELECT COUNT(*) FROM records")
    public int recordCount();

    @SqlQuery("SELECT * FROM records WHERE id = :id")
    public Pair<Long, String> lookupRecord(@Bind("id") int id);

    @SqlQuery("SELECT * FROM records WHERE created_date < :time LIMIT 10000")
    public List<Pair<Long, String>> lookupRecordsCreatedBefore(@Bind("time") Timestamp timestamp);

    @FetchSize(Integer.MIN_VALUE)
    @SqlQuery("SELECT * FROM records LIMIT 10000")
    public ResultIterator<Pair<Long, String>> lookupAllRecords();
  }

  public static class RecordPairMapper implements RowMapper<Pair<Long, String>> {
    @Override
    public Pair<Long, String> map(ResultSet r, StatementContext ctx) throws SQLException {
      return new Pair<>(r.getDate("created_date").getTime(), r.getString("value"));
    }
  }
}
