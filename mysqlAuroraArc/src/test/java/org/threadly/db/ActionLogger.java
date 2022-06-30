package org.threadly.db;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.threadly.concurrent.UnfairExecutor;
import org.threadly.util.Clock;
import org.threadly.util.StringUtils;
import org.threadly.util.StackSuppressedRuntimeException;

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
  
      Jdbi dbi = Jdbi.create(new HikariDataSource(c));
      dbi.installPlugin(new SqlObjectPlugin());
      DAO = dbi.onDemand(JdbiDao.class);
    } else {
      Class.forName(LoggingDriver.class.getName());
      Jdbi dbi = Jdbi.create("jdbc:mysql:logging://127.0.0.1:3306/auroraArc?useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                             "auroraArc", "");
      dbi.installPlugin(new SqlObjectPlugin());
      DAO = dbi.open().attach(JdbiDao.class);
    }
  }

  @AfterClass
  public static void cleanupClass() {
    System.out.println("-- CLOSING --");
    DAO.getHandle().close();
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
  public void readOnlyHandle() throws SQLException {
    System.out.println("-- STARTING: readOnlyHandle --");
    DAO.withHandle((handle) -> {
      handle.getConnection().setReadOnly(true);
      handle.getConnection().setReadOnly(false);
      return null;
    });
  }

  @Ignore // jdbi3 fails to load the last page as it immediately close the iterator
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
    DAO.inTransaction((dao) -> {
      dao.lookupRecord(1);
      dao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      return dao.recordCount();
    });
  }

  @Test (expected = StackSuppressedRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownBeforeAnyAction() {
    System.out.println("-- STARTING: transactionInsertAndLookupExceptionThrownBeforeAnyAction --");
    DAO.inTransaction((dao) -> {
      throw new StackSuppressedRuntimeException();
    });
  }

  @Test (expected = StackSuppressedRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterLookup() {
    System.out.println("-- STARTING: transactionInsertAndLookupExceptionThrownAfterLookup --");
    DAO.inTransaction((dao) -> {
      dao.lookupRecord(1);
      throw new StackSuppressedRuntimeException();
    });
  }

  @Test (expected = StackSuppressedRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterDone() {
    System.out.println("-- STARTING: transactionInsertAndLookupExceptionThrownAfterDone --");
    DAO.inTransaction((dao) -> {
      dao.lookupRecord(1);
      dao.insertRecordAndReturnId(StringUtils.makeRandomString(5));
      throw new StackSuppressedRuntimeException();
    });
  }

  @RegisterRowMapper(RecordMapper.class)
  public interface JdbiDao extends Transactional<JdbiDao> {
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public void insertRecord(@Bind("record") String record);

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
    public TestRecord lookupRecord(@Bind("id") int id);

    @SqlQuery("SELECT * FROM records WHERE created_date < :time")
    public List<TestRecord> lookupRecordsCreatedBefore(@Bind("time") Timestamp timestamp);

    @FetchSize(Integer.MIN_VALUE)
    @SqlQuery("SELECT * FROM records")
    public ResultIterator<TestRecord> lookupAllRecords();
  }

  public static class TestRecord {
    public final int id;
    public final String value;
    public final long timestamp;
    
    public TestRecord(int id, String value, long timestamp) {
      this.id = id;
      this.value = value;
      this.timestamp = timestamp;
    }
  }

  public static class RecordMapper implements RowMapper<TestRecord> {
    @Override
    public TestRecord map(ResultSet r, StatementContext ctx) throws SQLException {
      return new TestRecord(r.getInt("id"), r.getString("value"), r.getDate("created_date").getTime());
    }
  }
}
