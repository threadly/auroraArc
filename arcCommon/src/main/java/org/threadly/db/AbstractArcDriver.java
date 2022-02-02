package org.threadly.db;

/**
 * Small abstract driver for defining constants for threadly/arc drivers.
 */
public abstract class AbstractArcDriver implements java.sql.Driver {
  public static final int ARC_MAJOR_VERSION = 0;
  public static final int ARC_MINOR_VERSION = 17;
  
  @Override
  public int getMajorVersion() {
    return ARC_MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return ARC_MINOR_VERSION;
  }
}
