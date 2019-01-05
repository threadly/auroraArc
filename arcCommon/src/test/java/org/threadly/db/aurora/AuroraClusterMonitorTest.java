package org.threadly.db.aurora;

import static org.junit.Assert.fail;

import org.junit.Test;

public class AuroraClusterMonitorTest {
  @Test (expected = IllegalArgumentException.class)
  public void setServerCheckDelayMillisFail() {
    AuroraClusterMonitor.setServerCheckDelayMillis(0);
    fail("Exception expected");
  }
}
