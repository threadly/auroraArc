package org.threadly.db.aurora;

import java.util.Properties;

/**
 * Information about an Aurora server so that it can be referenced (and if needed connected to).
 */
public class AuroraServer implements Comparable<AuroraServer> {
  
  private final String host;
  private final int port;
  private final Properties info;
  private final int hashCode;
  private int weight = 1;

  /**
   * Construct a new {@link AuroraServer} for a given {@code host:post} string.
   * 
   * @param host Host and port within a single string
   * @param driver delegate, only currently used to get the default port
   * @param info Properties for the connection
   */
  public AuroraServer(String host, DelegateAuroraDriver driver,  Properties info) {
    int delim = host.indexOf(':');
    if (delim > 0) {
      this.host = host.substring(0, delim).intern();
      port = Integer.parseInt(host.substring(delim + 1));
    } else {
      this.host = host.intern();
      port = driver.getDefaultPort();
    }
    this.info = info; // not currently considered in equality or hash

    hashCode = this.host.hashCode() ^ port;
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

    hashCode = this.host.hashCode() ^ port;
  }
  
  protected void setWeight(int weight) {
    this.weight = weight;
  }
  
  protected int getWeight() {
    return weight;
  }
  
  /**
   * Return the hostname for this server.
   * 
   * @return Hostname for the server instance
   */
  public String getHost() {
    return host;
  }
  
  /**
   * Return the port for this server.
   * 
   * @return Port for the server instance
   */
  public int getPort() {
    return port;
  }
  
  /**
   * Check if the host and port matches to this server.
   * 
   * @param host Network host which must match exactly
   * @param port Server port used on host
   * @return {@code true} if both host and port match exactly
   */
  public boolean matchHost(String host, int port) {
    return this.host.equals(host) && this.port == port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof AuroraServer) {
      AuroraServer as = (AuroraServer)o;
      /*if (hashCode != as.hashCode) {
        return false;
      }*/
      return matchHost(as.host, as.port);
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
