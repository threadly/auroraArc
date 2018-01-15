package org.threadly.db.aurora;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Threadly's AuroraArc Driver.  This Driver will create multiple connections for each returned 
 * connection it provides.  Using these connections to monitor the aurora state, and to distribute 
 * queries to multiple aurora servers when possible.
 * <p>
 * This is different from most SQL drivers, in that there is shared state between all returned 
 * connections.  This shared state allows things like fail conditions to be communicated quickly, 
 * allowing for intelligence of how to mitigate problems, and use potential secondary / slave 
 * servers as soon as possible.
 * <p>
 * In general the user does not need to concern themselves with this benefit.  Just be aware that 
 * multiple connections will be established for every connection returned by this Driver.
 * <p>
 * Possible URL configuration options:
 * <ul>
 * <li>{@code "optimizedStateUpdates=true"} - Experimental internal code that can provide performance gains
 * </ul>
 */
public class Driver extends NonRegisteringDriver {
  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      throw new RuntimeException("Can't register driver!", e);
    }
  }
  
  /**
   * Another way to register the driver. This is more convenient than `Class.forName(String)` as no exceptions need to
   * be handled (instead just relying on the compile time dependency).
   */
  public static void registerDriver() {
    // Nothing needed, just a nicer way to initialize the static registration compared to Class.forName.
  }
  
  /**
   * Construct a new driver and register it with {@link DriverManager}.
   */
  public Driver() {
    // Required for Class.forName().newInstance()
  }
}
