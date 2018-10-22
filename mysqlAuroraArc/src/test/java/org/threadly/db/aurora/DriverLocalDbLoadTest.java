package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.skife.jdbi.v2.Handle;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.util.SuppressedStackRuntimeException;

@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING) // tests prefixed `a|z[0-9]_` to set order where it matters
public class DriverLocalDbLoadTest {
  private static final int RUN_COUNT_REFERENCE = 2_000;
  
  @BeforeClass
  public static void setupClass() {
    DriverLocalDbTest.setupClass();
  }
  
  @After
  public void cleanup() throws InterruptedException {
    Thread.sleep(200);
  }
  
  private void runLoad(boolean contendThreads, int threadCount, int perThreadRunCount, 
                       Consumer<DriverLocalDbTest> task) {
    PriorityScheduler ps = null;
    SubmitterExecutor executor = 
        threadCount > 1 ? (ps = new PriorityScheduler(threadCount)) : SameThreadSubmitterExecutor.instance();
    List<DriverLocalDbTest> toCleanup = new ArrayList<>(threadCount);
    DriverLocalDbTest test = null;
    for (int t = 0; t < threadCount; t++) {
      boolean constructed;
      if (constructed = (test == null)) {
        test = new DriverLocalDbTest();
        toCleanup.add(test);
        test.setup();
      }
      final DriverLocalDbTest fTest = test;
      if (! constructed || ! contendThreads) {
        test = null;
      }
      executor.execute(() -> {
        for (int i = 0; i < perThreadRunCount; i++) {
          try {
            task.accept(fTest);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
    if (ps != null) {
      ps.shutdown();
      try {
        ps.awaitTermination();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    for (DriverLocalDbTest dt : toCleanup) {
      dt.cleanup();
    }
  }

  @Test
  public void a1_insertRecord() {
    runLoad(true, 4, RUN_COUNT_REFERENCE / 2, (s) -> s.a1_insertRecord());
  }

  @Test
  public void a1_insertRecordAndReturnId() {
    runLoad(false, 2, RUN_COUNT_REFERENCE, (s) -> s.a1_insertRecordAndReturnId());
  }

  @Test
  public void a1_insertRecordInterfaceTransactionAndCount() {
    runLoad(false, 1, RUN_COUNT_REFERENCE / 2, 
            (s) -> s.a1_insertRecordInterfaceTransactionAndCount());
  }

  @Test
  public void a3_lookupSingleRecord() {
    runLoad(true, 32, RUN_COUNT_REFERENCE * 100, (s) -> s.a3_lookupSingleRecord());
  }

  @Test
  public void lookupMissingRecord() {
    runLoad(true, 32, RUN_COUNT_REFERENCE * 100, (s) -> s.lookupMissingRecord());
  }

  @Test
  public void transactionInsertAndLookupExceptionThrownBeforeAnyAction() {
    runLoad(true, 16, RUN_COUNT_REFERENCE * 100, (s) -> {
      try {
        s.transactionInsertAndLookupExceptionThrownBeforeAnyAction();
      } catch (SuppressedStackRuntimeException e) { /* expected */ }
    });
  }

  @Test
  public void transactionInsertAndLookupExceptionThrownAfterLookup() {
    runLoad(true, 16, RUN_COUNT_REFERENCE * 20, (s) -> {
      try {
        s.transactionInsertAndLookupExceptionThrownAfterLookup();
      } catch (SuppressedStackRuntimeException e) { /* expected */ }
    });
  }

  @Test
  public void transactionInsertAndLookupExceptionThrownAfterDone() {
    runLoad(true, 16, RUN_COUNT_REFERENCE, (s) -> {
      try {
        s.transactionInsertAndLookupExceptionThrownAfterDone();
      } catch (SuppressedStackRuntimeException e) { /* expected */ }
    });
  }

  @Test
  public void z_lookupRecordsPaged() {
    runLoad(true, 16, RUN_COUNT_REFERENCE / 10, (s) -> {
      try {
        s.z_lookupRecordsPaged();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void z_lookupRecordsCollection() {
    runLoad(true, 16, RUN_COUNT_REFERENCE / 100, (s) -> s.z_lookupRecordsCollection());
  }

  @Test
  public void a2_transactionInsertAndLookup() {
    runLoad(true, 1, RUN_COUNT_REFERENCE, (s) -> s.a2_transactionInsertAndLookup());
  }

  @Test 
  public void flipReadOnly() throws SQLException {
    try (Handle h = DriverLocalDbTest.DBI.open()) {
      Connection c = h.getConnection();
      for (int i = 0; i < RUN_COUNT_REFERENCE * 200_000; i++) {
        c.setReadOnly(i % 2 == 0);
      }
    }
  }

  @Test
  public void flipTransactionLevel() throws SQLException {
    try (Handle h = DriverLocalDbTest.DBI.open()) {
      Connection c = h.getConnection();
      for (int i = 0; i < RUN_COUNT_REFERENCE * 200_000; i++) {
        c.setTransactionIsolation((i % 2) + 1);
      }
    }
  }

  @Test
  public void openClose() throws SQLException {
    runLoad(true, 2, RUN_COUNT_REFERENCE * 2, (s) -> {
      Handle h = DriverLocalDbTest.DBI.open();
      h.getConnection();
      h.close();
    });
  }

  @Test
  public void getDelegateReadOnlyAutoCommit() throws SQLException {
    getDelegate(true, true);
  }

  @Test
  public void getDelegate() throws SQLException {
    getDelegate(false, false);
  }
  
  public void getDelegate(boolean autoCommit, boolean readOnly) throws SQLException {
    try (Handle h = DriverLocalDbTest.DBI.open()) {
      Connection c = h.getConnection();
      if (c instanceof DelegatingAuroraConnection) {
        c.setAutoCommit(autoCommit);
        c.setReadOnly(readOnly);
        DelegatingAuroraConnection dac = (DelegatingAuroraConnection)c;
        for (int i = 0; i < RUN_COUNT_REFERENCE * 200_000; i++) {
          dac.getDelegate();
          dac.resetStickyConnection();
        }
      }
    }
  }
}
