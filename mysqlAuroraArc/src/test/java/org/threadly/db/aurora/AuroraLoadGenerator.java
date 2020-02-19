package org.threadly.db.aurora;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.result.ResultIterator;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.FetchSize;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transactional;
import org.slf4j.LoggerFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.db.LoggingDriver;
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class AuroraLoadGenerator {
  private static final boolean SMALL_RUN = true;
  private static final int ALL_RECORD_ITERATOR_COUNT = SMALL_RUN ? 0 : 2;
  private static final int INSERT_COUNT = SMALL_RUN ? 1 : 64;
  private static final int SELECT_COUNT = SMALL_RUN ? 1 : 64;
  private static final int RESCHEDULE_DELAY = 10; // slower for debugging
  private static final boolean LOG_DRIVER = false;
  private static final boolean POOLED = true;
  
  public static void main(String[] args) throws Exception {
    PriorityScheduler scheduler = new PriorityScheduler(64, false);
    scheduler.prestartAllThreads();
    scheduler.setPoolSize(1024);
    
    //turnOffLogger("com.zaxxer.hikari.pool.HikariPool");
    turnOffLogger("com.zaxxer.hikari.pool.ProxyConnection");
    turnOffLogger("com.zaxxer.hikari.pool.PoolBase");
    
    startLoadTasks(scheduler, "127.0.0.1",//"auroraarc.cojltqybxnei.us-west-2.rds.amazonaws.com:3306,auroraarc-1.cojltqybxnei.us-west-2.rds.amazonaws.com:3306,auroraarc-2.cojltqybxnei.us-west-2.rds.amazonaws.com:3306",
                   "auroraArc", "", "auroraArc");
    scheduler.awaitTermination();
  }
  
  private static void turnOffLogger(String logger) {
    ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(logger)).setLevel(ch.qos.logback.classic.Level.OFF);
  }
  
  private static void startLoadTasks(PrioritySchedulerService scheduler, 
                                     String servers, String user, String password, String dbName) {
    if (LOG_DRIVER) {
      LoggingDriver.registerDriver();
    } else {
      Driver.registerDriver();
    }
    
    final LongAdder errorCount = new LongAdder();
    final LongAdder completeCount = new LongAdder();
    scheduler.scheduleAtFixedRate(new Runnable() {
      private long lastErrorCount = 0;
      private long lastCompleteCount = 0;
      private long lastRunTime = 0;
      @Override
      public void run() {
        long currentErrorCount = errorCount.sum();
        long currentCompleteCount = completeCount.sum();
        long newErrors = currentErrorCount - lastErrorCount;
        long newComplete = currentCompleteCount - lastCompleteCount;
        
        double secondsElapsed = (Clock.accurateForwardProgressingMillis() - lastRunTime) / 1000.0;
        lastRunTime = Clock.lastKnownForwardProgressingMillis();
        lastErrorCount = currentErrorCount;
        lastCompleteCount = currentCompleteCount;
        
        System.err.println("Error count: " + currentErrorCount + 
                             ", Error rate: " + (newErrors / secondsElapsed) + 
                             ", Complete rate: " + (newComplete / secondsElapsed));
      }
    }, 10_000, 10_000);
    
    Jdbi dbi;
    if (POOLED) {
      HikariConfig c = new HikariConfig();
      c.setUsername(user);
      c.setPassword(password);
      c.setJdbcUrl((LOG_DRIVER ? LoggingDriver.URL_PREFIX :"jdbc:mysql:aurora://") + servers + "/" + dbName + 
                     "?connectTimeout=10000&socketTimeout=10000&useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC");
      c.setDriverClassName(Driver.class.getName());
      if (SMALL_RUN) {
        c.setMaximumPoolSize(8);
        c.setMinimumIdle(8);
      } else {
        c.setMaximumPoolSize(64);
        c.setMinimumIdle(64);
      }
      c.setIdleTimeout(0);
      c.setConnectionTimeout(600_000);
      c.setLeakDetectionThreshold(0);
      c.setPoolName("auroraArc");
  
      dbi = Jdbi.create(new HikariDataSource(c));
    } else {
      dbi = Jdbi.create(LoggingDriver.URL_PREFIX + servers + "/" + dbName + 
                          "?connectTimeout=10000&socketTimeout=10000&useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC",
                        user, password);
    }
    dbi.installPlugin(new SqlObjectPlugin());
    
    for (int i = 0; i < ALL_RECORD_ITERATOR_COUNT; i++) {
      final JdbiDao dao = POOLED ? dbi.onDemand(JdbiDao.class) : dbi.open().attach(JdbiDao.class);
      scheduler.scheduleWithFixedDelay(() -> {
        int count = 0;
        try {
          Iterator<?> it = dao.lookupAllRecords();
          while (it.hasNext()) {
            count++;
            it.next(); // ignore
          }
          completeCount.increment();
        } catch (Exception e) {
          errorCount.increment();
        }
        System.out.println("Paged over " + count + " entries");
      }, i * 1000, 1000);
    }

    final AtomicInteger insertedCount = new AtomicInteger();
    final AtomicInteger selectedCount = new AtomicInteger();
    final Queue<Pair<Integer, String>> toQueryQueue = new ConcurrentLinkedQueue<>(); 
    
    for (int i = 0; i < INSERT_COUNT; i++) {
      final JdbiDao dao = POOLED ? dbi.onDemand(JdbiDao.class) : dbi.open().attach(JdbiDao.class);
      scheduler.scheduleWithFixedDelay(() -> {
        try {
          final String value = StringUtils.makeRandomString(10);
          final int id = dao.insertRecordAndReturnId("fooValue");
          toQueryQueue.add(new Pair<>(id, value));
          if (insertedCount.incrementAndGet() % 1000 == 0) {
            System.out.println("Inserted count: " + insertedCount.get());
          }
          completeCount.increment();
        } catch (Exception e) {
          errorCount.increment();
        }
      }, i * 100, RESCHEDULE_DELAY);
    }
    
    for (int i = 0; i < SELECT_COUNT; i++) {
      final JdbiDao dao = POOLED ? dbi.onDemand(JdbiDao.class) : dbi.open().attach(JdbiDao.class);
      scheduler.scheduleWithFixedDelay(() -> {
        Pair<Integer, String> p = toQueryQueue.poll();
        if (p != null) {
          try {
            String lookupValue = dao.lookupRecord(p.getLeft()).getRight();
            if (lookupValue.equals(p.getRight())) {
              throw new IllegalStateException("Expected: " + p.getRight() + ", got: " + lookupValue);
            }
            if (selectedCount.incrementAndGet() % 1000 == 0) {
              System.out.println("Selected count: " + selectedCount.get());
            }
            completeCount.increment();
          } catch (Exception e) {
            errorCount.increment();
          }
        }
      }, i * 200, RESCHEDULE_DELAY);
    }
    
    System.out.println("All db tasks submitted");
  }

  @RegisterRowMapper(RecordPairMapper.class)
  public abstract static class JdbiDao implements Transactional<JdbiDao> {
    @GetGeneratedKeys
    @SqlUpdate("INSERT INTO records (value, created_date) VALUES (:record, NOW())")
    public abstract int insertRecordAndReturnId(@Bind("record") String record);

    @SqlQuery("SELECT * FROM records WHERE id = :id")
    public abstract Pair<Long, String> lookupRecord(@Bind("id") int id);

    @FetchSize(Integer.MIN_VALUE)
    @SqlQuery("SELECT * FROM records")
    public abstract ResultIterator<Pair<Long, String>> lookupAllRecords();

    public abstract void close();
  }

  public static class RecordPairMapper implements RowMapper<Pair<Long, String>> {
    @Override
    public Pair<Long, String> map(ResultSet r, StatementContext ctx) throws SQLException {
      return new Pair<>(r.getDate("created_date").getTime(), r.getString("value"));
    }
  }
}
