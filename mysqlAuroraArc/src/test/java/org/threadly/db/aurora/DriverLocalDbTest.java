package org.threadly.db.aurora;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.FetchSize;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.threadly.concurrent.UnfairExecutor;
import org.threadly.db.LoggingDriver;
import org.threadly.db.aurora.Driver;
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;
import org.threadly.util.SuppressedStackRuntimeException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) // tests prefixed `a|z[0-9]_` to set order where it matters
public class DriverLocalDbTest {
  private static final boolean LOGGING_DRIVER = false;
  private static final boolean OPTIMIZED_DRIVER = true;
  protected static DBI DBI;

  @BeforeClass
  public static void setupClass() {
    if (LOGGING_DRIVER) {
      LoggingDriver.registerDriver();
    } else {
      Driver.registerDriver();
    }
    DBI = new DBI("jdbc:mysql:" + (LOGGING_DRIVER ? "logging" : "aurora") + "://" +  
                    "127.0.0.1:3306,127.0.0.2:3306,127.0.0.3:3306,127.0.0.4:3306" + 
                    "/auroraArc?" + 
                    "useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false" + 
                    (OPTIMIZED_DRIVER ? "&optimizedStateUpdates=true" : ""),
                  "auroraArc", "");
  }
  
  protected Handle h;
  protected JdbiDao dao;

  @Before
  public void setup() {
    h = DBI.open();
    dao = h.attach(JdbiDao.class);
  }

  @After
  public void cleanup() {
    dao.close();
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
  public void a1_insertRecord() {
    dao.insertRecord(StringUtils.makeRandomString(5));
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
    dao.inTransaction((txDao, txStatus) -> {
      txDao.lookupRecord(1);
      txDao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      return txDao.recordCount();
    });
  }

  @Test
  public void a3_lookupSingleRecord() {
    Pair<Long, String> p = dao.lookupRecord(1);
    assertNotNull(p);
  }

  @Test
  public void lookupMissingRecord() {
    Pair<Long, String> p = dao.lookupRecord(-1);
    assertNull(p);
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownBeforeAnyAction() {
    dao.inTransaction((txDao, txStatus) -> {
      throw new SuppressedStackRuntimeException();
    });
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterLookup() {
    dao.inTransaction((txDao, txStatus) -> {
      txDao.lookupRecord(1);
      throw new SuppressedStackRuntimeException();
    });
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterDone() {
    dao.inTransaction((txDao, txStatus) -> {
      txDao.lookupRecord(1);
      txDao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      throw new SuppressedStackRuntimeException();
    });
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

  @RegisterMapper(RecordPairMapper.class)
  public abstract static class JdbiDao implements Transactional<JdbiDao> {
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public abstract void insertRecord(@Bind("record") String record);

    @SqlUpdate("DELETE FROM records WHERE id != 1")
    public abstract void deleteRecords();

    @GetGeneratedKeys
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public abstract int insertRecordAndReturnId(@Bind("record") String record);

    @Transaction (value = TransactionIsolationLevel.SERIALIZABLE)
    public int insertRecordAndReturnCount(String record) {
      insertRecordAndReturnId(record);
      return recordCount();
    }

    @SqlQuery("SELECT COUNT(*) FROM records")
    public abstract int recordCount();

    @SqlQuery("SELECT * FROM records WHERE id = :id")
    public abstract Pair<Long, String> lookupRecord(@Bind("id") int id);

    @SqlQuery("SELECT * FROM records WHERE created_date < :time LIMIT 10000")
    public abstract List<Pair<Long, String>> lookupRecordsCreatedBefore(@Bind("time") Timestamp timestamp);

    @FetchSize(Integer.MIN_VALUE)
    @SqlQuery("SELECT * FROM records LIMIT 10000")
    public abstract ResultIterator<Pair<Long, String>> lookupAllRecords();

    public abstract void close();
  }

  public static class RecordPairMapper implements ResultSetMapper<Pair<Long, String>> {
    @Override
    public Pair<Long, String> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
      return new Pair<>(r.getDate("created_date").getTime(), r.getString("value"));
    }
  }
}
