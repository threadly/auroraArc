package org.threadly.db.aurora;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

public class AuroraDataSourceFactory implements ObjectFactory {
  /**
   * The class name for a standard MySQL DataSource.
   */
  protected final static String DATA_SOURCE_CLASS_NAME = AuroraDataSource.class.getName();
  
  @Override
  public Object getObjectInstance(Object refObj, Name nm, Context ctx, Hashtable<?, ?> env) throws Exception {
    Reference ref = (Reference) refObj;
    String className = ref.getClassName();
    
    if ((className != null) && (className.equals(DATA_SOURCE_CLASS_NAME))) {
      final AuroraDataSource dataSource = (AuroraDataSource) Class.forName(className).newInstance();
      
      int portNumber = 3306;
      
      String portNumberAsString = nullSafeRefAddrStringGet("port", ref);
      
      if (portNumberAsString != null) {
        portNumber = Integer.parseInt(portNumberAsString);
      }
      
      dataSource.setPort(portNumber);
      
      String user = nullSafeRefAddrStringGet("user", ref);
      
      if (user != null) {
        dataSource.setUser(user);
      }
      
      String password = nullSafeRefAddrStringGet("password", ref);
      
      if (password != null) {
        dataSource.setPassword(password);
      }
      
      String serverName = nullSafeRefAddrStringGet("serverName", ref);
      
      if (serverName != null) {
        dataSource.setServerName(serverName);
      }
      
      String databaseName = nullSafeRefAddrStringGet("databaseName", ref);
      
      if (databaseName != null) {
        dataSource.setDatabaseName(databaseName);
      }
      
      String explicitUrlAsString = nullSafeRefAddrStringGet("explicitUrl", ref);
      
      if (explicitUrlAsString != null && Boolean.parseBoolean(explicitUrlAsString)) {
        dataSource.setUrl(nullSafeRefAddrStringGet("url", ref));
      }
      
      dataSource.setPropertiesViaRef(ref);
      
      return dataSource;
    }
    
    // We can't create an instance of the reference
    return null;
  }
  
  private String nullSafeRefAddrStringGet(String referenceName, Reference ref) {
    RefAddr refAddr = ref.get(referenceName);
    return (refAddr != null) ? (String) refAddr.getContent() : null;
  }
}
