package org.threadly.db;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
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
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;
import org.threadly.util.SuppressedStackRuntimeException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) // tests prefixed `a|z[0-9]_` to set order where it matters
public class ActionLogger {
  private static JdbiDao DAO;

  @BeforeClass
  public static void setupClass() throws ClassNotFoundException {
    Class.forName(LoggingDriver.class.getName());
    DBI dbi = new DBI("jdbc:mysql:logging://127.0.0.1:3306/auroraArc?useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC",
                      "auroraArc", "");
    System.out.println("-- OPENING --");
    DAO = dbi.open(JdbiDao.class);
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
      for (int i = 0; i < 10_000; i++) {
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
    Iterator<?> it = DAO.lookupAllRecords();
    while (it.hasNext()) {
      it.next(); // ignore value
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

  @RegisterMapper(RecordPairMapper.class)
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
    public abstract Pair<Long, String> lookupRecord(@Bind("id") int id);

    @SqlQuery("SELECT * FROM records WHERE created_date < :time")
    public abstract List<Pair<Long, String>> lookupRecordsCreatedBefore(@Bind("time") Timestamp timestamp);

    @FetchSize(Integer.MIN_VALUE)
    @SqlQuery("SELECT * FROM records")
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
