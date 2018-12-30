package org.threadly.db;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.util.StringUtils;

public class WrappingDatabaseMetaDataTest {
  private DatabaseMetaData mockMD;
  private WrappingDatabaseMetaData wrapMD;
  
  @Before
  public void setup() {
    mockMD = mock(DatabaseMetaData.class);
    wrapMD = new TestWrappingDatabaseMetaData(mockMD);
  }
  
  @After
  public void cleanup() {
    mockMD = null;
  }
  
  @Test
  public void driverVersionTest() {
    assertEquals(AbstractArcDriver.ARC_MAJOR_VERSION, wrapMD.getDriverMajorVersion());
    assertEquals(AbstractArcDriver.ARC_MINOR_VERSION, wrapMD.getDriverMinorVersion());
  }

  @Test
  public void noArgumentFunctionPassthroughTest() throws Exception {
    for (Method m : DatabaseMetaData.class.getMethods()) {
      if (m.getParameterTypes().length == 0 &&  
          ! m.getName().equals("wait") && ! m.getName().startsWith("notify")) {
        m.invoke(wrapMD);
      }
    }
    DatabaseMetaData verifyMock = verify(mockMD);
    for (Method m :verifyMock .getClass().getMethods()) {
      if (m.getParameterTypes().length == 0 && ! m.getName().startsWith("getDriver") && 
          ! m.getName().equals("wait") && ! m.getName().startsWith("notify") && 
          ! m.getName().equals("toString")) {
        m.invoke(verifyMock);
      }
    }
  }
  
  @Test
  public void isWrapperForTest() throws SQLException {
    wrapMD.isWrapperFor(this.getClass());
    verify(mockMD).isWrapperFor(this.getClass());
  }

  @Test
  public void unwrapTest() throws SQLException {
    wrapMD.unwrap(this.getClass());
    verify(mockMD).unwrap(this.getClass());
  }

  @Test
  public void deletesAreDetectedTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.deletesAreDetected(type);
    verify(mockMD).deletesAreDetected(type);
  }

  @Test
  public void getAttributesTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String typeNamePattern = StringUtils.makeRandomString(5);
    String attributeNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    verify(mockMD).getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
  }

  @Test
  public void getBestRowIdentifierTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    int scope = ThreadLocalRandom.current().nextInt();
    boolean nullable = ThreadLocalRandom.current().nextBoolean();
    wrapMD.getBestRowIdentifier(catalog, schema, table, scope, nullable);
    verify(mockMD).getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }
  
  @Test
  public void getColumnPrivilegesTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    String columnNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getColumnPrivileges(catalog, schema, table, columnNamePattern);
    verify(mockMD).getColumnPrivileges(catalog, schema, table, columnNamePattern);
  }

  @Test
  public void getColumnsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String tableNamePattern = StringUtils.makeRandomString(5);
    String columnNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    verify(mockMD).getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Test
  public void getCrossReferenceTest() throws SQLException {
    String parentCatalog = StringUtils.makeRandomString(5);
    String parentSchema = StringUtils.makeRandomString(5);
    String parentTable = StringUtils.makeRandomString(5);
    String foreignCatalog = StringUtils.makeRandomString(5);
    String foreignSchema = StringUtils.makeRandomString(5);
    String foreignTable = StringUtils.makeRandomString(5);
    wrapMD.getCrossReference(parentCatalog, parentSchema, parentTable, 
                             foreignCatalog, foreignSchema, foreignTable);
    verify(mockMD).getCrossReference(parentCatalog, parentSchema, parentTable, 
                                     foreignCatalog, foreignSchema, foreignTable);
  }

  @Test
  public void getExportedKeysTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    wrapMD.getExportedKeys(catalog, schema, table);
    verify(mockMD).getExportedKeys(catalog, schema, table);
  }

  @Test
  public void getFunctionColumnsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String functionNamePattern = StringUtils.makeRandomString(5);
    String columnNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
  }

  @Test
  public void getFunctionsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5); 
    String functionNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getFunctions(catalog, schemaPattern, functionNamePattern);
    verify(mockMD).getFunctions(catalog, schemaPattern, functionNamePattern);
  }

  @Test
  public void getImportedKeysTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    wrapMD.getImportedKeys(catalog, schema, table);
    verify(mockMD).getImportedKeys(catalog, schema, table);
  }

  @Test
  public void getIndexInfoTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    boolean unique = ThreadLocalRandom.current().nextBoolean();
    boolean approximate = ThreadLocalRandom.current().nextBoolean();
    wrapMD.getIndexInfo(catalog, schema, table, unique, approximate);
    verify(mockMD).getIndexInfo(catalog, schema, table, unique, approximate);
  }

  @Test
  public void getPrimaryKeysTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    wrapMD.getPrimaryKeys(catalog, schema, table);
    verify(mockMD).getPrimaryKeys(catalog, schema, table);
  }

  @Test
  public void getProcedureColumnsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String procedureNamePattern = StringUtils.makeRandomString(5);
    String columnNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    verify(mockMD).getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
  }

  @Test
  public void getProceduresTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String procedureNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getProcedures(catalog, schemaPattern, procedureNamePattern);
    verify(mockMD).getProcedures(catalog, schemaPattern, procedureNamePattern);
  }

  @Test
  public void getPseudoColumnsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String tableNamePattern = StringUtils.makeRandomString(5);
    String columnNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    verify(mockMD).getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Test
  public void getSchemasTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    wrapMD.getSchemas(catalog, schemaPattern);
    verify(mockMD).getSchemas(catalog, schemaPattern);
  }

  @Test
  public void getSuperTablesTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String tableNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getSuperTables(catalog, schemaPattern, tableNamePattern);
    verify(mockMD).getSuperTables(catalog, schemaPattern, tableNamePattern);
  }

  @Test
  public void getSuperTypesTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String typeNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getSuperTypes(catalog, schemaPattern, typeNamePattern);
    verify(mockMD).getSuperTypes(catalog, schemaPattern, typeNamePattern);
  }

  @Test
  public void getTablePrivilegesTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String tableNamePattern = StringUtils.makeRandomString(5);
    wrapMD.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
    verify(mockMD).getTablePrivileges(catalog, schemaPattern, tableNamePattern);
  }

  @Test
  public void getTablesTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String tableNamePattern = StringUtils.makeRandomString(5);
    String[] types = new String[] { StringUtils.makeRandomString(5) };
    wrapMD.getTables(catalog, schemaPattern, tableNamePattern, types);
    verify(mockMD).getTables(catalog, schemaPattern, tableNamePattern, types);
  }

  @Test
  public void getUDTsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schemaPattern = StringUtils.makeRandomString(5);
    String typeNamePattern = StringUtils.makeRandomString(5);
    int[] types = new int[] { ThreadLocalRandom.current().nextInt() };
    wrapMD.getUDTs(catalog, schemaPattern, typeNamePattern, types);
    verify(mockMD).getUDTs(catalog, schemaPattern, typeNamePattern, types);
  }

  @Test
  public void getVersionColumnsTest() throws SQLException {
    String catalog = StringUtils.makeRandomString(5);
    String schema = StringUtils.makeRandomString(5);
    String table = StringUtils.makeRandomString(5);
    wrapMD.getVersionColumns(catalog, schema, table);
    verify(mockMD).getVersionColumns(catalog, schema, table);
  }

  @Test
  public void insertsAreDetectedTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.insertsAreDetected(type);
    verify(mockMD).insertsAreDetected(type);
  }

  @Test
  public void othersDeletesAreVisibleTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.othersDeletesAreVisible(type);
    verify(mockMD).othersDeletesAreVisible(type);
  }

  @Test
  public void othersInsertsAreVisibleTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.othersInsertsAreVisible(type);
    verify(mockMD).othersInsertsAreVisible(type);
  }

  @Test
  public void othersUpdatesAreVisibleTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.othersUpdatesAreVisible(type);
    verify(mockMD).othersUpdatesAreVisible(type);
  }

  @Test
  public void ownDeletesAreVisibleTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.ownDeletesAreVisible(type);
    verify(mockMD).ownDeletesAreVisible(type);
  }

  @Test
  public void ownInsertsAreVisibleTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.ownInsertsAreVisible(type);
    verify(mockMD).ownInsertsAreVisible(type);
  }

  @Test
  public void ownUpdatesAreVisibleTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.ownUpdatesAreVisible(type);
    verify(mockMD).ownUpdatesAreVisible(type);
  }

  @Test
  public void supportsConvertTest() throws SQLException {
    int fromType = ThreadLocalRandom.current().nextInt();
    int toType = ThreadLocalRandom.current().nextInt();
    wrapMD.supportsConvert(fromType, toType);
    verify(mockMD).supportsConvert(fromType, toType);
  }

  @Test
  public void supportsResultSetConcurrencyTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    int concurrency = ThreadLocalRandom.current().nextInt();
    wrapMD.supportsResultSetConcurrency(type, concurrency);
    verify(mockMD).supportsResultSetConcurrency(type, concurrency);
  }

  @Test
  public void supportsResultSetHoldabilityTest() throws SQLException {
    int holdability = ThreadLocalRandom.current().nextInt();
    wrapMD.supportsResultSetHoldability(holdability);
    verify(mockMD).supportsResultSetHoldability(holdability);
  }

  @Test
  public void supportsResultSetTypeTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.supportsResultSetType(type);
    verify(mockMD).supportsResultSetType(type);
  }

  @Test
  public void supportsTransactionIsolationLevelTest() throws SQLException {
    int level = ThreadLocalRandom.current().nextInt();
    wrapMD.supportsTransactionIsolationLevel(level);
    verify(mockMD).supportsTransactionIsolationLevel(level);
  }

  @Test
  public void updatesAreDetectedTest() throws SQLException {
    int type = ThreadLocalRandom.current().nextInt();
    wrapMD.updatesAreDetected(type);
    verify(mockMD).updatesAreDetected(type);
  }
  
  private static class TestWrappingDatabaseMetaData extends WrappingDatabaseMetaData {
    public TestWrappingDatabaseMetaData(DatabaseMetaData delegate) {
      super(delegate);
    }

    @Override
    public String getDriverName() throws SQLException {
      // pass through for test
      return delegate.getDriverName();
    }
  }
}
