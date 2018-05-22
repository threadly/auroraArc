package org.threadly.db;

/**
 * Small abstract driver for defining constants for threadly/arc drivers.
 */
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
