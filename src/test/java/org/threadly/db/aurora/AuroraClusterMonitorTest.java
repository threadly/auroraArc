package org.threadly.db.aurora;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.db.aurora.DelegateMockDriver.MockDriver;

public class AuroraClusterMonitorTest {
  private MockDriver mockDriver;

  @Before
  public void setup() {
    mockDriver = DelegateMockDriver.setupMockDriverAsDelegate();
  }

  @After
  public void cleanup() {
    DelegateMockDriver.resetDriver();
    mockDriver = null;
  }

  private Set<AuroraServer> makeDummySet() {
    Set<AuroraServer> result = new HashSet<>();
    result.add(new AuroraServer("host1", new Properties()));
    result.add(new AuroraServer("host2", new Properties()));
    return result;
  }

  @Test
  public void getMonitorConsistentResultTest() {
    AuroraClusterMonitor monitor1 = AuroraClusterMonitor.getMonitor(makeDummySet());
    AuroraClusterMonitor monitor2 = AuroraClusterMonitor.getMonitor(makeDummySet());
    assertTrue(monitor1 == monitor2);
  }

  @Test
  public void getRandomReadReplicaTest() {
    // TODO
    fail();
  }

  @Test
  public void getRandomReadReplicaMasterOnlyTest() {
    // TODO
    fail();
  }

  @Test
  public void getCurrentMasterTest() {
    // TODO
    fail();
  }

  @Test
  public void expiditeServerCheckTest() {
    // TODO
    fail();
  }
}
