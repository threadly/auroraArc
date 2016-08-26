package org.threadly.db;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.threadly.db.mysql.MySqlConnection;

public class Driver implements java.sql.Driver {
  private static final List<ConnectionFactory> CONNECTION_FACTORIES;
  
  static {
    ArrayList<ConnectionFactory> connectionFactories = new ArrayList<ConnectionFactory>();
    connectionFactories.add(new ConnectionFactory() {
      @Override
      public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:mysql:");
      }

      @Override
      public Connection makeConnection(String url, Properties info) {
        return new MySqlConnection(url, info);
      }
    });
    connectionFactories.trimToSize();
    CONNECTION_FACTORIES = Collections.unmodifiableList(connectionFactories);
  }
  
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    for (ConnectionFactory factory : CONNECTION_FACTORIES) {
      if (factory.acceptsURL(url)) {
        return factory.makeConnection(url, info);
      }
    }
    throw new UnsupportedOperationException("Could not find connection factory for url: " + url);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    for (ConnectionFactory factory : CONNECTION_FACTORIES) {
      if (factory.acceptsURL(url)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public boolean jdbcCompliant() {
    // XXX not sure if we will ever want to be fully compliant?
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  private static interface ConnectionFactory {
    public boolean acceptsURL(String url);
    
    public Connection makeConnection(String url, Properties info);
  }
}
