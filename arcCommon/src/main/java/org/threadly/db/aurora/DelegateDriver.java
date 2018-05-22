package org.threadly.db.aurora;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.threadly.util.Pair;

/**
 * Driver for creating connections to a given delegate implementation.  This ultimately deals with 
 * creating a URL that is suited for that driver and then using it to establish the connection.
 */
public abstract class DelegateDriver {
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static final Pair<String, DelegateDriver>[] DEFAULT_IMPLEMENTATIONS = 
    new Pair[] {attemptInitialization("org.threadly.db.aurora.mysql.MySqlDelegateDriver"), 
                attemptInitialization("org.threadly.db.aurora.psql.PsqlDelegateDriver")};
  
  private static Pair<String, DelegateDriver> attemptInitialization(String delegateClass) {
    try {
      return new Pair<>(delegateClass, (DelegateDriver)Class.forName(delegateClass).newInstance());
    } catch (ClassNotFoundException e) {
      return new Pair<>(delegateClass, null);
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Get driver instance for a given URL if one is available.  This is dependent on delegate 
   * driver implementation being included in the classpath from other artifacts. 
   * 
   * @param url JDBC connect URL
   * @return A {@link DelegateDriver} if one could be found, or {@code null}
   */
  public static DelegateDriver driverForArcUrl(String url) {
    if (url == null) {
      return null;
    }
    for (Pair<String, DelegateDriver> p : DEFAULT_IMPLEMENTATIONS) {
      if (p.getRight() != null && url.startsWith(p.getRight().arcPrefix)) {
        return p.getRight();
      }
    }
    return null;
  }

  /**
   * Returns any loaded DelegateDriver.  It should be a TODO to remove use of this as much as 
   * possible since its use may be problematic if a single JVM needs to connect to both types of 
   * aurora instances.
   * 
   * @return The first loaded delegate driver it can find
   */
  public static DelegateDriver getAnyDelegateDriver() {
    for (Pair<String, DelegateDriver> p : DEFAULT_IMPLEMENTATIONS) {
      if (p.getRight() != null) {
        return p.getRight();
      }
    }
    return null;
  }

  protected final String arcPrefix;
  protected final String driverConnectPrefix;
  protected final java.sql.Driver delegateDriver;
  
  protected DelegateDriver(String arcPrefix, String driverConnectPrefix, 
                           java.sql.Driver delegateDriver) {
    this.arcPrefix = arcPrefix;
    this.driverConnectPrefix = driverConnectPrefix;
    this.delegateDriver = delegateDriver;
  }
  
  /**
   * Getter for the jdbc url prefix used for the auroraArc driver.
   * 
   * @return The prefix used for connecting to this delegate with auroraArc
   */
  public String getArcPrefix() {
    return arcPrefix;
  }

  /**
   * Get the delegated driver instance.
   * 
   * @return Instance of driver to delegate actual connections to
   */
  public java.sql.Driver getDriver() {
    return delegateDriver;
  }

  /**
   * Connect using the delegate driver.
   * 
   * @param hostAndArgs Host and JDBC connect arguments WITHOUT the JDBC prefix
   * @param info Connection properties
   * @return A new connection or {@code null} if delegate is incorrectly configured
   * @throws SQLException Thrown if the delegate throws while connecting
   */
  public Connection connect(String hostAndArgs, Properties info) throws SQLException {
    return delegateDriver.connect(driverConnectPrefix + hostAndArgs, info);
  }
}
