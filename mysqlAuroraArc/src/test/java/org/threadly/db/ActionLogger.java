package org.threadly.db;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.skife.jdbi.v2.DBI;
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
import org.skife.jdbi.v2.sqlobject.mixins.GetHandle;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.threadly.concurrent.UnfairExecutor;
import org.threadly.db.LoggingDriver;
import org.threadly.util.Clock;
import org.threadly.util.StringUtils;
import org.threadly.util.SuppressedStackRuntimeException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) // tests prefixed `a|z[0-9]_` to set order where it matters
public class ActionLogger {
  private static final boolean POOLED = true;
  
  private static JdbiDao DAO;

  @BeforeClass
  public static void setupClass() throws ClassNotFoundException {
    System.out.println("-- OPENING --");
    if (POOLED) {
      HikariConfig c = new HikariConfig();
      c.setUsername("auroraArc");
      c.setPassword("");
      c.setJdbcUrl("jdbc:mysql:logging://127.0.0.1:3306/auroraArc?useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false");
      c.setDriverClassName(LoggingDriver.class.getName());
      c.setMaximumPoolSize(1);
      c.setMinimumIdle(1);
      c.setIdleTimeout(0);
      c.setConnectionTimeout(600_000);
      c.setLeakDetectionThreshold(0);
      c.setPoolName("auroraArc");
  
      DBI dbi = new DBI(new HikariDataSource(c));
      DAO = dbi.onDemand(JdbiDao.class);
    } else {
      Class.forName(LoggingDriver.class.getName());
      DBI dbi = new DBI("jdbc:mysql:logging://127.0.0.1:3306/auroraArc?useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                        "auroraArc", "");
      DAO = dbi.open(JdbiDao.class);
    }
  }

  @AfterClass
  public static void cleanupClass() {
    System.out.println("-- CLOSING --");
    DAO.close();
    System.out.println("-- DONE --");
  }

  @Test
  @Ignore
  public void a0_setup() throws InterruptedException {
    PrintStream originalPrintStream = System.out;
    originalPrintStream.println("Surpressing output during setup");
    try {
      System.setOut(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          // ignored
        }
      }));
      UnfairExecutor executor = new UnfairExecutor(31);
      for (int i = 0; i < 8000_000; i++) {
        executor.execute(() -> DAO.insertRecord(StringUtils.makeRandomString(10)));
      }
      executor.shutdown();
      executor.awaitTermination();
    } finally {
      System.setOut(originalPrintStream);
      System.out.println("Setup done!");
    }
  }

  @Test
  public void a1_insertRecord() {
    System.out.println("-- STARTING: insertRecord --");
    DAO.insertRecord(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordAndReturnId() {
    System.out.println("-- STARTING: insertRecordAndReturnId --");
    DAO.insertRecordAndReturnId(StringUtils.makeRandomString(5));
  }

  @Test
  public void a1_insertRecordInterfaceTransaction() {
    System.out.println("-- STARTING: insertRecordInterfaceTransaction --");
    DAO.insertRecordAndReturnCount(StringUtils.makeRandomString(5));
  }

  @Test
  public void lookupSingleRecord() {
    System.out.println("-- STARTING: lookupSingleRecord --");
    DAO.lookupRecord(1);
  }
  
  @Test
  public void readOnlyHandle() {
    System.out.println("-- STARTING: readOnlyHandle --");
    DAO.withHandle((handle) -> {
      handle.getConnection().setReadOnly(true);
      handle.getConnection().setReadOnly(false);
      return null;
    });
  }

  @Test
  public void z_lookupRecordsPaged() throws InterruptedException {
    System.out.println("-- STARTING: lookupRecordsPaged --");
    try (ResultIterator<TestRecord> it = DAO.lookupAllRecords()) {
      while (it.hasNext()) {
        it.next();
      }
    }
  }

  @Test
  public void z_lookupRecordsCollection() {
    System.out.println("-- STARTING: lookupRecordsCollection --");
    DAO.lookupRecordsCreatedBefore(new Timestamp(Clock.lastKnownTimeMillis() -
                                                   Clock.accurateForwardProgressingMillis()));
  }

  @Test
  public void a2_transactionInsertAndLookup() {
    System.out.println("-- STARTING: transactionInsertAndLookup --");
    DAO.inTransaction((rt, dao) -> {
      DAO.lookupRecord(1);
      DAO.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      return DAO.recordCount();
    });
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownBeforeAnyAction() {
    System.out.println("-- STARTING: transactionInsertAndLookupExceptionThrownBeforeAnyAction --");
    DAO.inTransaction((rt, dao) -> {
      throw new SuppressedStackRuntimeException();
    });
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterLookup() {
    System.out.println("-- STARTING: transactionInsertAndLookupExceptionThrownAfterLookup --");
    DAO.inTransaction((rt, dao) -> {
      DAO.lookupRecord(1);
      throw new SuppressedStackRuntimeException();
    });
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterDone() {
    System.out.println("-- STARTING: transactionInsertAndLookupExceptionThrownAfterDone --");
    DAO.inTransaction((rt, dao) -> {
      DAO.lookupRecord(1);
      DAO.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      throw new SuppressedStackRuntimeException();
    });
  }

  @RegisterMapper(RecordMapper.class)
  public abstract static class JdbiDao implements Transactional<JdbiDao>, GetHandle {
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public abstract void insertRecord(@Bind("record") String record);

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
    public abstract TestRecord lookupRecord(@Bind("id") int id);

    @SqlQuery("SELECT * FROM records WHERE created_date < :time")
    public abstract List<TestRecord> lookupRecordsCreatedBefore(@Bind("time") Timestamp timestamp);

    @FetchSize(Integer.MIN_VALUE)
    @SqlQuery("SELECT * FROM records")
    public abstract ResultIterator<TestRecord> lookupAllRecords();

    public abstract void close();
  }

  @SuppressWarnings("unused")
  private static class TestRecord {
    public final int id;
    public final String value;
    public final long timestamp;
    
    public TestRecord(int id, String value, long timestamp) {
      this.id = id;
      this.value = value;
      this.timestamp = timestamp;
    }
  }

  public static class RecordMapper implements ResultSetMapper<TestRecord> {
    @Override
    public TestRecord map(int index, ResultSet r, StatementContext ctx) throws SQLException {
      return new TestRecord(r.getInt("id"), r.getString("value"), r.getDate("created_date").getTime());
    }
  }
}
