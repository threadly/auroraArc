package org.threadly.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Wraps a {@link DatabaseMetaData}, delegating to it for most functions.  This class should be 
 * extended with the functions needing mutations overridden.
 * 
 * @since 0.8
 */
public abstract class WrappingDatabaseMetaData implements DatabaseMetaData {
  protected final DatabaseMetaData delegate;
  
  /**
   * Construct a new {@link WrappingDatabaseMetaData} with the provided delegate.
   * 
   * @param delegate Delegate to use for non-overridden functions
   */
  public WrappingDatabaseMetaData(DatabaseMetaData delegate) {
    this.delegate = delegate;
  }

  @Override
  public int getDriverMajorVersion() { // we always assume the threadly arc version is desired here
    return AbstractArcDriver.ARC_MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() { // we always assume the threadly arc version is desired here
    return AbstractArcDriver.ARC_MINOR_VERSION;
  }
  
  // getDriverName must be implemented per-driver

  @Override
  public String getDriverVersion() throws SQLException {
    return getDriverName() + "-" + AbstractArcDriver.ARC_MAJOR_VERSION + "." + AbstractArcDriver.ARC_MINOR_VERSION;
  }

  // START functions with default implementations
  
  @Override
  public long getMaxLogicalLobSize() throws SQLException {
    return delegate.getMaxLogicalLobSize();
  }
  
  @Override
  public boolean supportsRefCursors() throws SQLException {
    return delegate.supportsRefCursors();
  }
  
  // END functions with with default implementations

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return delegate.isWrapperFor(iface);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return delegate.unwrap(iface);
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return delegate.allProceduresAreCallable();
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return delegate.allTablesAreSelectable();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return delegate.autoCommitFailureClosesAllResultSets();
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return delegate.dataDefinitionCausesTransactionCommit();
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return delegate.dataDefinitionIgnoredInTransactions();
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return delegate.deletesAreDetected(type);
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return delegate.doesMaxRowSizeIncludeBlobs();
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return delegate.generatedKeyAlwaysReturned();
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                 String attributeNamePattern) throws SQLException {
    return delegate.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                        boolean nullable) throws SQLException {
    return delegate.getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return delegate.getCatalogSeparator();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return delegate.getCatalogTerm();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return delegate.getCatalogs();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return delegate.getClientInfoProperties();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                       String columnNamePattern) throws SQLException {
    return delegate.getColumnPrivileges(catalog, schema, table, columnNamePattern);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                              String columnNamePattern) throws SQLException {
    return delegate.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return delegate.getConnection();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                     String parentTable, String foreignCatalog,
                                     String foreignSchema, String foreignTable)
                                                                                throws SQLException {
    return delegate.getCrossReference(parentCatalog, parentSchema, parentTable, 
                                      foreignCatalog, foreignSchema, foreignTable);
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return delegate.getDatabaseMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return delegate.getDatabaseMinorVersion();
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return delegate.getDatabaseProductName();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return delegate.getDatabaseProductVersion();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return delegate.getDefaultTransactionIsolation();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return delegate.getExportedKeys(catalog, schema, table);
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return delegate.getExtraNameCharacters();
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern, 
                                      String columnNamePattern) throws SQLException {
    return delegate.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, 
                                String functionNamePattern) throws SQLException {
    return delegate.getFunctions(catalog, schemaPattern, functionNamePattern);
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return delegate.getIdentifierQuoteString();
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return delegate.getImportedKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                boolean approximate) throws SQLException {
    return delegate.getIndexInfo(catalog, schema, table, unique, approximate);
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return delegate.getJDBCMajorVersion();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return delegate.getJDBCMinorVersion();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return delegate.getMaxBinaryLiteralLength();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return delegate.getMaxCatalogNameLength();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return delegate.getMaxCharLiteralLength();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return delegate.getMaxColumnNameLength();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return delegate.getMaxColumnsInGroupBy();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return delegate.getMaxColumnsInIndex();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return delegate.getMaxColumnsInOrderBy();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return delegate.getMaxColumnsInSelect();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return delegate.getMaxColumnsInTable();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return delegate.getMaxConnections();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return delegate.getMaxCursorNameLength();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return delegate.getMaxIndexLength();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return delegate.getMaxProcedureNameLength();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return delegate.getMaxRowSize();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return delegate.getMaxSchemaNameLength();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return delegate.getMaxStatementLength();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return delegate.getMaxStatements();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return delegate.getMaxTableNameLength();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return delegate.getMaxTablesInSelect();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return delegate.getMaxUserNameLength();
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return delegate.getNumericFunctions();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return delegate.getPrimaryKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern, String columnNamePattern) throws SQLException {
    return delegate.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return delegate.getProcedureTerm();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern,
                                 String procedureNamePattern) throws SQLException {
    return delegate.getProcedures(catalog, schemaPattern, procedureNamePattern);
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                    String columnNamePattern) throws SQLException {
    return delegate.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return delegate.getResultSetHoldability();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return delegate.getRowIdLifetime();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return delegate.getSQLKeywords();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return delegate.getSQLStateType();
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return delegate.getSchemaTerm();
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return delegate.getSchemas();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return delegate.getSchemas(catalog, schemaPattern);
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return delegate.getSearchStringEscape();
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return delegate.getStringFunctions();
  }

  @Override
  public ResultSet getSuperTables(String catalog, 
                                  String schemaPattern, String tableNamePattern) throws SQLException {
    return delegate.getSuperTables(catalog, schemaPattern, tableNamePattern);
  }

  @Override
  public ResultSet getSuperTypes(String catalog, 
                                 String schemaPattern, String typeNamePattern) throws SQLException {
    return delegate.getSuperTypes(catalog, schemaPattern, typeNamePattern);
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return delegate.getSystemFunctions();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                      String tableNamePattern) throws SQLException {
    return delegate.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return delegate.getTableTypes();
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                             String[] types) throws SQLException {
    return delegate.getTables(catalog, schemaPattern, tableNamePattern, types);
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return delegate.getTimeDateFunctions();
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return delegate.getTypeInfo();
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                           int[] types) throws SQLException {
    return delegate.getUDTs(catalog, schemaPattern, typeNamePattern, types);
  }

  @Override
  public String getURL() throws SQLException {
    return delegate.getURL();
  }

  @Override
  public String getUserName() throws SQLException {
    return delegate.getUserName();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return delegate.getVersionColumns(catalog, schema, table);
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return delegate.insertsAreDetected(type);
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return delegate.isCatalogAtStart();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return delegate.isReadOnly();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return delegate.locatorsUpdateCopy();
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return delegate.nullPlusNonNullIsNull();
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return delegate.nullsAreSortedAtEnd();
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return delegate.nullsAreSortedAtStart();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return delegate.nullsAreSortedHigh();
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return delegate.nullsAreSortedLow();
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return delegate.othersDeletesAreVisible(type);
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return delegate.othersInsertsAreVisible(type);
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return delegate.othersUpdatesAreVisible(type);
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return delegate.ownDeletesAreVisible(type);
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return delegate.ownInsertsAreVisible(type);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return delegate.ownUpdatesAreVisible(type);
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return delegate.storesLowerCaseIdentifiers();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return delegate.storesLowerCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return delegate.storesMixedCaseIdentifiers();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return delegate.storesMixedCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return delegate.storesUpperCaseIdentifiers();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return delegate.storesUpperCaseQuotedIdentifiers();
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return delegate.supportsANSI92EntryLevelSQL();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return delegate.supportsANSI92FullSQL();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return delegate.supportsANSI92IntermediateSQL();
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return delegate.supportsAlterTableWithAddColumn();
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return delegate.supportsAlterTableWithDropColumn();
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return delegate.supportsBatchUpdates();
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return delegate.supportsCatalogsInDataManipulation();
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return delegate.supportsCatalogsInIndexDefinitions();
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return delegate.supportsCatalogsInPrivilegeDefinitions();
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return delegate.supportsCatalogsInProcedureCalls();
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return delegate.supportsCatalogsInTableDefinitions();
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return delegate.supportsColumnAliasing();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return delegate.supportsConvert();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return delegate.supportsConvert(fromType, toType);
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return delegate.supportsCoreSQLGrammar();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return delegate.supportsCorrelatedSubqueries();
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return delegate.supportsDataDefinitionAndDataManipulationTransactions();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return delegate.supportsDataManipulationTransactionsOnly();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return delegate.supportsDifferentTableCorrelationNames();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return delegate.supportsExpressionsInOrderBy();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return delegate.supportsExtendedSQLGrammar();
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return delegate.supportsFullOuterJoins();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return delegate.supportsGetGeneratedKeys();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return delegate.supportsGroupBy();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return delegate.supportsGroupByBeyondSelect();
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return delegate.supportsGroupByUnrelated();
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return delegate.supportsIntegrityEnhancementFacility();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return delegate.supportsLikeEscapeClause();
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return delegate.supportsLimitedOuterJoins();
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return delegate.supportsMinimumSQLGrammar();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return delegate.supportsMixedCaseIdentifiers();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return delegate.supportsMixedCaseQuotedIdentifiers();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return delegate.supportsMultipleOpenResults();
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return delegate.supportsMultipleResultSets();
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return delegate.supportsMultipleTransactions();
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return delegate.supportsNamedParameters();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return delegate.supportsNonNullableColumns();
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return delegate.supportsOpenCursorsAcrossCommit();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return delegate.supportsOpenCursorsAcrossRollback();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return delegate.supportsOpenStatementsAcrossCommit();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return delegate.supportsOpenStatementsAcrossRollback();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return delegate.supportsOrderByUnrelated();
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return delegate.supportsOuterJoins();
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return delegate.supportsPositionedDelete();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return delegate.supportsPositionedUpdate();
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return delegate.supportsResultSetConcurrency(type, concurrency);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return delegate.supportsResultSetHoldability(holdability);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return delegate.supportsResultSetType(type);
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return delegate.supportsSavepoints();
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return delegate.supportsSchemasInDataManipulation();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return delegate.supportsSchemasInIndexDefinitions();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return delegate.supportsSchemasInPrivilegeDefinitions();
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return delegate.supportsSchemasInProcedureCalls();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return delegate.supportsSchemasInTableDefinitions();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return delegate.supportsSelectForUpdate();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return delegate.supportsStatementPooling();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return delegate.supportsStoredFunctionsUsingCallSyntax();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return delegate.supportsStoredProcedures();
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return delegate.supportsSubqueriesInComparisons();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return delegate.supportsSubqueriesInExists();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return delegate.supportsSubqueriesInIns();
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return delegate.supportsSubqueriesInQuantifieds();
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return delegate.supportsTableCorrelationNames();
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return delegate.supportsTransactionIsolationLevel(level);
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return delegate.supportsTransactions();
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return delegate.supportsUnion();
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return delegate.supportsUnionAll();
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return delegate.updatesAreDetected(type);
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return delegate.usesLocalFilePerTable();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return delegate.usesLocalFiles();
  }
}