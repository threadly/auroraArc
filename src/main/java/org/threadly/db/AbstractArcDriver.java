package org.threadly.db;

public abstract class AbstractArcDriver implements java.sql.Driver {
  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }
}
