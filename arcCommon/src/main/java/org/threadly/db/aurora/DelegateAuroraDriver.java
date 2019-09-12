package org.threadly.db.aurora;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.threadly.util.Pair;
import org.threadly.util.StackSuppressedRuntimeException;

/**
 * Driver for creating connections to a given delegate implementation.  This ultimately deals with 
 * creating a URL that is suited for that driver and then using it to establish the connection.
 */
public abstract class DelegateAuroraDriver {
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static final Pair<String, DelegateAuroraDriver>[] DEFAULT_IMPLEMENTATIONS = 
    new Pair[] { attemptInitialization("org.threadly.db.aurora.mysql.MySqlDelegateDriver"), 
                 attemptInitialization("org.threadly.db.aurora.psql.PsqlDelegateDriver") };
  
  private static Pair<String, DelegateAuroraDriver> attemptInitialization(String delegateClass) {
    try {
      return new Pair<>(delegateClass, 
                        (DelegateAuroraDriver)Class.forName(delegateClass)
                                                   .getDeclaredConstructor().newInstance());
    } catch (ClassNotFoundException e) {
      return new Pair<>(delegateClass, null);
    } catch (InvocationTargetException | NoSuchMethodException | 
             IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Get driver instance for a given URL if one is available.  This is dependent on delegate 
   * driver implementation being included in the classpath from other artifacts. 
   * 
   * @param url JDBC connect URL
   * @return A {@link DelegateAuroraDriver} if one could be found, or {@code null}
   */
  public static DelegateAuroraDriver driverForArcUrl(String url) {
    if (url == null) {
      return null;
    }
    for (Pair<String, DelegateAuroraDriver> p : DEFAULT_IMPLEMENTATIONS) {
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
  public static DelegateAuroraDriver getAnyDelegateDriver() {
    for (Pair<String, DelegateAuroraDriver> p : DEFAULT_IMPLEMENTATIONS) {
      if (p.getRight() != null) {
        return p.getRight();
      }
    }
    return null;
  }

  protected final String arcPrefix;
  protected final String driverConnectPrefix;
  protected final java.sql.Driver delegateDriver;
  
  protected DelegateAuroraDriver(String arcPrefix, String driverConnectPrefix, 
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
   * Get the parameters specific for the connection used for checking the master / slave / health 
   * check.  By default this will return a timezone of UTC and SSL disabled.  Each parameter should 
   * be prefixed / delineated with {@code &}, this includes the first parameter.  If no parameters are 
   * provided this should return the empty string.
   * 
   * @return A {@code non-null} list of parameters to affix to the status check URL
   */
  public String getStatusConnectURLParams() {
    return "&serverTimezone=UTC&useSSL=false";
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
   * Get the name for this driver.
   * 
   * @return The name for the driver
   */
  public abstract String getDriverName();

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

  /**
   * Check if the provided server and matching connection are a master server for this drivers 
   * implementation.
   * 
   * @param server The server the connection is associated to
   * @param serverConnection The connection to execute against the server
   * @return {@code true} if the provided server and connection are cluster master
   * @throws SQLException Thrown if there is an error communicating with the server
   */
  public abstract boolean isMasterServer(AuroraServer server, Connection serverConnection) throws SQLException;
  
  /**
   * Exception to indicate that the driver is in an invalid state.  Possibly because the 
   * protocol with the backend is not what the driver expected.
   */
  protected static class IllegalDriverStateException extends StackSuppressedRuntimeException {
    private static final long serialVersionUID = -6537317025944243741L;

    public IllegalDriverStateException(String msg) {
      super(msg);
    }
  }
}
