package org.threadly.db.aurora;

import java.util.Properties;

/**
 * Information about an Aurora server so that it can be referenced (and if needed connected to).
 */
public class AuroraServer implements Comparable<AuroraServer> {
  private static final int DEFAULT_PORT = 3306;
  
  private final String host;
  private final int port;
  private final Properties info;
  private final int hashCode;

  /**
   * Construct a new {@link AuroraServer} for a given {@code host:post} string.
   * 
   * @param server Host and port within a single string
   * @param info Properties for the connection
   */
  public AuroraServer(String server, Properties info) {
    int delim = server.indexOf(':');
    if (delim > 0) {
      host = server.substring(0, delim).intern();
      port = Integer.parseInt(server.substring(delim + 1));
    } else {
      host = server.intern();
      port = DEFAULT_PORT;
    }
    this.info = info; // not currently considered in equality or hash

    hashCode = host.hashCode() ^ port;
  }

  /**
   * Construct a new {@link AuroraServer} with the host and port broken into individual components.
   * 
   * @param host Server host / dns
   * @param port Port to connect to
   * @param info Properties for the connection
   */
  public AuroraServer(String host, int port, Properties info) {
    this.host = host.intern();
    this.port = port;
    this.info = info; // not currently considered in equality or hash

    hashCode = host.hashCode() ^ port;
  }
  
  /**
   * Return the hostname for this server.
   * 
   * @return Hostname for server instance
   */
  public String getHost() {
    return host;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof AuroraServer) {
      AuroraServer as = (AuroraServer)o;
      /*if (hashCode != as.hashCode) {
        return false;
      } else */if (! host.equals(as.host)) {
        return false;
      } else if (port != as.port) {
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public int compareTo(AuroraServer as) {
    int hostCompare = host.compareTo(as.host);
    if (hostCompare == 0) {
      return port - as.port;
    } else {
      return hostCompare;
    }
  }

  /**
   * Produce a {@code host:port} representation of the host to be connected to.
   * 
   * @return The destination host and port represented in a single string
   */
  public String hostAndPortString() {
    return host + ":" + port;
  }

  @Override
  public String toString() {
    return hostAndPortString();
  }

  /**
   * Return the properties for the connection.
   * 
   * @return Properties provided at construction
   */
  public Properties getProperties() {
    return info;
  }
}
