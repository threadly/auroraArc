package org.threadly.db.aurora;

import static org.junit.Assert.fail;

import org.junit.Test;

public class AuroraClusterMonitorTest {
  @Test (expected = IllegalArgumentException.class)
  public void setServerCheckDelayMillisFail() {
    AuroraClusterMonitor.setServerCheckDelayMillis(0);
    fail("Exception expected");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setReplicaWeightNegativeFail() {
    AuroraClusterMonitor.setHostWeight("host", 3360, -1);
    fail("Exception expected");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setReplicaWeightMaxFail() {
    AuroraClusterMonitor.setHostWeight("host", 3360, 101);
    fail("Exception expected");
  }
}
