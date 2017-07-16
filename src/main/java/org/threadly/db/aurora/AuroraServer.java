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

  public AuroraServer(String host, int port, Properties info) {
    this.host = host.intern();
    this.port = port;
    this.info = info; // not currently considered in equality or hash

    hashCode = host.hashCode() ^ port;
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

  public String hostAndPortString() {
    return host + ":" + port;
  }

  @Override
  public String toString() {
    return hostAndPortString();
  }

  public Properties getProperties() {
    return info;
  }
}
