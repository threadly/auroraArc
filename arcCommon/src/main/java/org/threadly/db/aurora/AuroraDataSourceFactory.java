package org.threadly.db.aurora;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.threadly.util.StringUtils;

/**
 * Implementation of {@link ObjectFactory} that provides a configured {@link AuroraDataSource}.
 * 
 * @since 0.5
 */
public class AuroraDataSourceFactory implements ObjectFactory {
  protected static final String DATA_SOURCE_CLASS_NAME = AuroraDataSource.class.getName();
  
  @Override
  public Object getObjectInstance(Object refObj, Name nm, Context ctx, Hashtable<?, ?> env) {
    Reference ref = (Reference) refObj;
    String className = ref.getClassName();
    if (StringUtils.isNullOrEmpty(className)) {
      return null;
    } else if (DATA_SOURCE_CLASS_NAME.equals(className)) {
      return new AuroraDataSource(ref);
    } else {
      return null;
    }
  }
}
