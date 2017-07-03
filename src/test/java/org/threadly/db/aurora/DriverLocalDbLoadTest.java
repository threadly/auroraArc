package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.skife.jdbi.v2.Handle;
import org.threadly.util.SuppressedStackRuntimeException;

@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING) // tests prefixed `a|z[0-9]_` to set order where it matters
public class DriverLocalDbLoadTest extends DriverLocalDbTest {
  private static final int RUN_COUNT_REFERENCE = 4_000;

  @Test
  public void a0_setup() throws InterruptedException {
    // must be defined to ensure run in correct order
    super.a0_setup();
  }
  
  @Test
  public void a1_insertRecord() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 2; i++) {
      super.a1_insertRecord();
    }
  }

  @Test
  public void a1_insertRecordAndReturnId() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 2; i++) {
      super.a1_insertRecordAndReturnId();
    }
  }

  @Test
  public void a1_insertRecordInterfaceTransactionAndCount() {
    for (int i = 0; i < RUN_COUNT_REFERENCE / 2; i++) {
      super.a1_insertRecordInterfaceTransactionAndCount();
    }
  }

  @Test
  public void a3_lookupSingleRecord() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 200; i++) {
      super.a3_lookupSingleRecord();
    }
  }

  @Test
  public void lookupMissingRecord() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 200; i++) {
      super.lookupMissingRecord();
    }
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownBeforeAnyAction() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 100_000; i++) {
      super.transactionInsertAndLookupExceptionThrownBeforeAnyAction();
    }
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterLookup() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 100_000; i++) {
      super.transactionInsertAndLookupExceptionThrownAfterLookup();
    }
  }

  @Test (expected = SuppressedStackRuntimeException.class)
  public void transactionInsertAndLookupExceptionThrownAfterDone() {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 100_000; i++) {
      super.transactionInsertAndLookupExceptionThrownAfterDone();
    }
  }

  @Test
  public void z_lookupRecordsPaged() throws InterruptedException {
    for (int i = 0; i < RUN_COUNT_REFERENCE / 20; i++) {
      super.z_lookupRecordsPaged();
    }
  }

  @Test
  public void z_lookupRecordsCollection() {
    for (int i = 0; i < RUN_COUNT_REFERENCE / 20; i++) {
      super.z_lookupRecordsCollection();
    }
  }

  @Test
  public void a2_transactionInsertAndLookup() {
    for (int i = 0; i < RUN_COUNT_REFERENCE; i++) {
      super.a2_transactionInsertAndLookup();
    }
  }

  @Test 
  public void flipReadOnly() throws SQLException {
    try (Handle h = DBI.open()) {
      Connection c = h.getConnection();
      for (int i = 0; i < RUN_COUNT_REFERENCE * 200_000; i++) {
        c.setReadOnly(i % 2 == 0);
      }
    }
  }

  @Test
  public void flipTransactionLevel() throws SQLException {
    try (Handle h = DBI.open()) {
      Connection c = h.getConnection();
      for (int i = 0; i < RUN_COUNT_REFERENCE * 100; i++) {
        c.setTransactionIsolation((i % 2) + 1);
      }
    }
  }

  @Test
  public void openClose() throws SQLException {
    for (int i = 0; i < RUN_COUNT_REFERENCE * 2; i++) {
      Handle h = DBI.open();
      h.getConnection();
      h.close();
    }
  }

  @Test
  public void getDelegate() throws SQLException {
    try (Handle h = DBI.open()) {
      Connection c = h.getConnection();
      if (c instanceof DelegatingAuroraConnection) {
        c.setAutoCommit(true);
        c.setReadOnly(true);
        DelegatingAuroraConnection dac = (DelegatingAuroraConnection)c;
        for (int i = 0; i < RUN_COUNT_REFERENCE * 200_000; i++) {
          dac.getDelegate();
        }
      }
    }
  }
}
