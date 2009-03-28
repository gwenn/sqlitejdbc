/*
 * Copyright (c) 2007 David Crawshaw <david@zentus.com>
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package org.sqlite;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

class MetaData implements DatabaseMetaData
{
    private Conn conn;
    private PreparedStatement
        getTables = null,
        getTableTypes = null,
        getTypeInfo = null,
        getCatalogs = null,
        getSchemas = null,
        getUDTs = null,
        getColumnsTblName = null,
        getSuperTypes = null,
        getSuperTables = null,
        getTablePrivileges = null,
        getExportedKeys = null,
        getProcedures = null,
        getProcedureColumns = null,
        getAttributes = null,
        getVersionColumns = null,
        getColumnPrivileges = null;

    /** Used by PrepStmt to save generating a new statement every call. */
    private PreparedStatement getGeneratedKeys = null;

    MetaData(Conn conn) { this.conn = conn; }

    void checkOpen() throws SQLException {
        if (conn == null) throw new SQLException("connection closed"); }

    synchronized void close() throws SQLException {
        if (conn == null) return;

        try {
            if (getTables != null) getTables.close();
            if (getTableTypes != null) getTableTypes.close();
            if (getTypeInfo != null) getTypeInfo.close();
            if (getCatalogs != null) getCatalogs.close();
            if (getSchemas != null) getSchemas.close();
            if (getUDTs != null) getUDTs.close();
            if (getColumnsTblName != null) getColumnsTblName.close();
            if (getSuperTypes != null) getSuperTypes.close();
            if (getSuperTables != null) getSuperTables.close();
            if (getTablePrivileges != null) getTablePrivileges.close();
            if (getExportedKeys != null) getExportedKeys.close();
            if (getProcedures != null) getProcedures.close();
            if (getProcedureColumns != null) getProcedureColumns.close();
            if (getAttributes != null) getAttributes.close();
            if (getVersionColumns != null) getVersionColumns.close();
            if (getColumnPrivileges != null) getColumnPrivileges.close();
            if (getGeneratedKeys != null) getGeneratedKeys.close();

            getTables = null;
            getTableTypes = null;
            getTypeInfo = null;
            getCatalogs = null;
            getSchemas = null;
            getUDTs = null;
            getColumnsTblName = null;
            getSuperTypes = null;
            getSuperTables = null;
            getTablePrivileges = null;
            getExportedKeys = null;
            getProcedures = null;
            getProcedureColumns = null;
            getAttributes = null;
            getVersionColumns = null;
            getColumnPrivileges = null;
            getGeneratedKeys = null;
        } finally {
            conn = null;
        }
    }

    public Connection getConnection() { return conn; }
    public int getDatabaseMajorVersion() { return 3; }
    public int getDatabaseMinorVersion() { return 0; }
    public int getDriverMajorVersion() { return 1; }
    public int getDriverMinorVersion() { return 1; }
    public int getJDBCMajorVersion() { return 2; }
    public int getJDBCMinorVersion() { return 1; }
    public int getDefaultTransactionIsolation()
        { return Connection.TRANSACTION_SERIALIZABLE; }
    public int getMaxBinaryLiteralLength() { return 0; }
    public int getMaxCatalogNameLength() { return 0; }
    public int getMaxCharLiteralLength() { return 0; }
    public int getMaxColumnNameLength() { return 0; }
    public int getMaxColumnsInGroupBy() { return 0; }
    public int getMaxColumnsInIndex() { return 0; }
    public int getMaxColumnsInOrderBy() { return 0; }
    public int getMaxColumnsInSelect() { return 0; }
    public int getMaxColumnsInTable() { return 0; }
    public int getMaxConnections() { return 0; }
    public int getMaxCursorNameLength() { return 0; }
    public int getMaxIndexLength() { return 0; }
    public int getMaxProcedureNameLength() { return 0; }
    public int getMaxRowSize() { return 0; }
    public int getMaxSchemaNameLength() { return 0; }
    public int getMaxStatementLength() { return 0; }
    public int getMaxStatements() { return 0; }
    public int getMaxTableNameLength() { return 0; }
    public int getMaxTablesInSelect() { return 0; }
    public int getMaxUserNameLength() { return 0; }
    public int getResultSetHoldability()
        { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    public int getSQLStateType() { return sqlStateSQL99; }

    public String getDatabaseProductName() { return "SQLite"; }
    public String getDatabaseProductVersion() throws SQLException {
        return conn.libversion();
    }
    public String getDriverName() { return "SQLiteJDBC"; }
    public String getDriverVersion() { return conn.getDriverVersion(); }
    public String getExtraNameCharacters() { return ""; }
    public String getCatalogSeparator() { return "."; }
    public String getCatalogTerm() { return "catalog"; }
    public String getSchemaTerm() { return "schema"; }
    public String getProcedureTerm() { return "not_implemented"; }
    public String getSearchStringEscape() { return null; }
    public String getIdentifierQuoteString() { return " "; }
    public String getSQLKeywords() { return ""; }
    public String getNumericFunctions() { return "abs,max,min,round,random"; }
    public String getStringFunctions() { return "glob,length,like,lower,ltrim,replace,rtrim,soundex,substr,trim,upper"; }
    public String getSystemFunctions() { return "last_insert_rowid,load_extension,sqlite_version"; }
    public String getTimeDateFunctions() { return "date,time,datetime,julianday,strftime"; }

    public String getURL() { return conn.url(); }
    public String getUserName() { return null; }

    public boolean allProceduresAreCallable() { return false; }
    public boolean allTablesAreSelectable() { return true; }
    public boolean dataDefinitionCausesTransactionCommit() { return false; }
    public boolean dataDefinitionIgnoredInTransactions() { return false; }
    public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    public boolean deletesAreDetected(int type) { return false; }
    public boolean insertsAreDetected(int type) { return false; }
    public boolean isCatalogAtStart() { return true; }
    public boolean locatorsUpdateCopy() { return false; }
    public boolean nullPlusNonNullIsNull() { return true; }
    public boolean nullsAreSortedAtEnd() { return !nullsAreSortedAtStart(); }
    public boolean nullsAreSortedAtStart() { return true; }
    public boolean nullsAreSortedHigh() { return true; }
    public boolean nullsAreSortedLow() { return !nullsAreSortedHigh(); }
    public boolean othersDeletesAreVisible(int type) { return false; }
    public boolean othersInsertsAreVisible(int type) { return false; }
    public boolean othersUpdatesAreVisible(int type) { return false; }
    public boolean ownDeletesAreVisible(int type) { return false; }
    public boolean ownInsertsAreVisible(int type) { return false; }
    public boolean ownUpdatesAreVisible(int type) { return false; }
    public boolean storesLowerCaseIdentifiers() { return false; }
    public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    public boolean storesMixedCaseIdentifiers() { return true; }
    public boolean storesMixedCaseQuotedIdentifiers() { return false; }
    public boolean storesUpperCaseIdentifiers() { return false; }
    public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    public boolean supportsAlterTableWithAddColumn() { return false; }
    public boolean supportsAlterTableWithDropColumn() { return false; }
    public boolean supportsANSI92EntryLevelSQL() { return false; }
    public boolean supportsANSI92FullSQL() { return false; }
    public boolean supportsANSI92IntermediateSQL() { return false; }
    public boolean supportsBatchUpdates() { return true; }
    public boolean supportsCatalogsInDataManipulation() { return false; }
    public boolean supportsCatalogsInIndexDefinitions() { return false; }
    public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    public boolean supportsCatalogsInProcedureCalls() { return false; }
    public boolean supportsCatalogsInTableDefinitions() { return false; }
    public boolean supportsColumnAliasing() { return true; }
    public boolean supportsConvert() { return false; }
    public boolean supportsConvert(int fromType, int toType) { return false; }
    public boolean supportsCorrelatedSubqueries() { return false; }
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        { return true; }
    public boolean supportsDataManipulationTransactionsOnly() { return false; }
    public boolean supportsDifferentTableCorrelationNames() { return false; }
    public boolean supportsExpressionsInOrderBy() { return true; }
    public boolean supportsMinimumSQLGrammar() { return true; }
    public boolean supportsCoreSQLGrammar() { return true; }
    public boolean supportsExtendedSQLGrammar() { return false; }
    public boolean supportsLimitedOuterJoins() { return true; }
    public boolean supportsFullOuterJoins() { return false; }
    public boolean supportsGetGeneratedKeys() { return false; }
    public boolean supportsGroupBy() { return true; }
    public boolean supportsGroupByBeyondSelect() { return false; }
    public boolean supportsGroupByUnrelated() { return false; }
    public boolean supportsIntegrityEnhancementFacility() { return false; }
    public boolean supportsLikeEscapeClause() { return false; }
    public boolean supportsMixedCaseIdentifiers() { return true; }
    public boolean supportsMixedCaseQuotedIdentifiers() { return false; }
    public boolean supportsMultipleOpenResults() { return false; }
    public boolean supportsMultipleResultSets() { return false; }
    public boolean supportsMultipleTransactions() { return true; }
    public boolean supportsNamedParameters() { return true; }
    public boolean supportsNonNullableColumns() { return true; }
    public boolean supportsOpenCursorsAcrossCommit() { return false; }
    public boolean supportsOpenCursorsAcrossRollback() { return false; }
    public boolean supportsOpenStatementsAcrossCommit() { return false; }
    public boolean supportsOpenStatementsAcrossRollback() { return false; }
    public boolean supportsOrderByUnrelated() { return false; }
    public boolean supportsOuterJoins() { return true; }
    public boolean supportsPositionedDelete() { return false; }
    public boolean supportsPositionedUpdate() { return false; }
    public boolean supportsResultSetConcurrency(int t, int c)
        { return t == ResultSet.TYPE_FORWARD_ONLY
              && c == ResultSet.CONCUR_READ_ONLY; }
    public boolean supportsResultSetHoldability(int h)
        { return h == ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    public boolean supportsResultSetType(int t)
        { return t == ResultSet.TYPE_FORWARD_ONLY; }
    public boolean supportsSavepoints() { return true; }
    public boolean supportsSchemasInDataManipulation() { return false; }
    public boolean supportsSchemasInIndexDefinitions() { return false; }
    public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    public boolean supportsSchemasInProcedureCalls() { return false; }
    public boolean supportsSchemasInTableDefinitions() { return false; }
    public boolean supportsSelectForUpdate() { return false; }
    public boolean supportsStatementPooling() { return false; }
    public boolean supportsStoredProcedures() { return false; }
    public boolean supportsSubqueriesInComparisons() { return false; }
    public boolean supportsSubqueriesInExists() { return true; } // TODO: check
    public boolean supportsSubqueriesInIns() { return true; } // TODO: check
    public boolean supportsSubqueriesInQuantifieds() { return false; }
    public boolean supportsTableCorrelationNames() { return false; }
    public boolean supportsTransactionIsolationLevel(int level)
        { return level == Connection.TRANSACTION_SERIALIZABLE || level == Connection.TRANSACTION_READ_UNCOMMITTED; }
    public boolean supportsTransactions() { return true; }
    public boolean supportsUnion() { return true; }
    public boolean supportsUnionAll() { return true; }
    public boolean updatesAreDetected(int type) { return false; }
    public boolean usesLocalFilePerTable() { return false; }
    public boolean usesLocalFiles() { return true; }
    public boolean isReadOnly() throws SQLException
        { return conn.isReadOnly(); }

    public ResultSet getAttributes(String c, String s, String t, String a)
            throws SQLException {
        if (getAttributes == null) getAttributes = conn.prepareStatement(
            "select "
            + "null as TYPE_CAT, "
            + "null as TYPE_SCHEM, "
            + "null as TYPE_NAME, "
            + "null as ATTR_NAME, "
            + "null as DATA_TYPE, "
            + "null as ATTR_TYPE_NAME, "
            + "null as ATTR_SIZE, "
            + "null as DECIMAL_DIGITS, "
            + "null as NUM_PREC_RADIX, "
            + "null as NULLABLE, "
            + "null as REMARKS, "
            + "null as ATTR_DEF, "
            + "null as SQL_DATA_TYPE, "
            + "null as SQL_DATETIME_SUB, "
            + "null as CHAR_OCTET_LENGTH, "
            + "null as ORDINAL_POSITION, "
            + "null as IS_NULLABLE, "
            + "null as SCOPE_CATALOG, "
            + "null as SCOPE_SCHEMA, "
            + "null as SCOPE_TABLE, "
            + "null as SOURCE_DATA_TYPE limit 0;");
        return getAttributes.executeQuery();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
            int scope, boolean nullable) throws SQLException {
        final StringBuffer sql = new StringBuffer();
        Statement stat = conn.createStatement();

        sql.append("select ").
            append(scope).append(" as SCOPE, ").
            append("cn as COLUMN_NAME, ").
            append("ct as DATA_TYPE, ").
            append("tn as TYPE_NAME, ").
            append("10 as COLUMN_SIZE, "). // FIXME
            append("0 as BUFFER_LENGTH, ").
            append("0 as DECIMAL_DIGITS, ").
            append("pc as PSEUDO_COLUMN from (");

        if (null != table) {
            boolean exists = false;
            int i = 0;
            String colName = null;
            String colType = null;
            ResultSet rs = null;
            try {
                rs = stat.executeQuery("pragma table_info("+escape(table)+");");
                exists = true;
                for (i=0; rs.next(); i++) {
                    if (!rs.getBoolean(6) || (!nullable && "0".equals(rs.getString(4)))) { i--; continue; }
                    if (i > 1) { break; }
                    colName = rs.getString(2);
                    colType = getSQLiteType(rs.getString(3));
                }
            } catch (SQLException e) {
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
            if (!exists) {
                sql.append("select null as cn, null as ct, null as tn, null as pc) limit 0;");
            } else if (i != 1) {
                sql.append("select ").
                    append("'ROWID' as cn, ").
                    append(Types.INTEGER).append(" as ct, ").
                    append("'INTEGER' as tn, ").
                    append(bestRowPseudo).append(" as pc) order by SCOPE;");
            } else {
                sql.append("select ").
                    append(escape(colName)).append(" as cn, ").
                    append(getJavaType(colType)).append(" as ct, ").
                    append("'").append(colType).append("' as tn, ").
                    append(bestRowNotPseudo).append(" as pc) order by SCOPE;");
            }
        } else {
            sql.append("select null as cn, null as ct, null as tn, null as pc) limit 0;");
        }

        return stat.executeQuery(sql.toString());
    }

    public ResultSet getColumnPrivileges(String c, String s, String t,
                                         String colPat)
            throws SQLException {
        if (getColumnPrivileges == null)
            getColumnPrivileges = conn.prepareStatement(
            "select "
            + "null as TABLE_CAT, "
            + "null as TABLE_SCHEM, "
            + "null as TABLE_NAME, "
            + "null as COLUMN_NAME, "
            + "null as GRANTOR, "
            + "null as GRANTEE, "
            + "null as PRIVILEGE, "
            + "null as IS_GRANTABLE limit 0;");
        return getColumnPrivileges.executeQuery();
    }

    public ResultSet getColumns(String c, String s, String tbl, String colPat)
            throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;
        final StringBuilder sql = new StringBuilder();

        checkOpen();

        if (getColumnsTblName == null)
            getColumnsTblName = conn.prepareStatement(
                "select tbl_name from sqlite_master where tbl_name like ?;");

        // determine exact table name
        getColumnsTblName.setString(1, tbl);
        rs = getColumnsTblName.executeQuery();
        if (!rs.next())
            return rs;
        tbl = rs.getString(1);
        rs.close();

        sql.append("select ").
            append("null as TABLE_CAT, ").
            append("null as TABLE_SCHEM, ").
            append(escape(tbl)).append(" as TABLE_NAME, ").
            append("cn as COLUMN_NAME, ").
            append("ct as DATA_TYPE, ").
            append("tn as TYPE_NAME, ").
            append("2000000000 as COLUMN_SIZE, "). // FIXME
            append("2000000000 as BUFFER_LENGTH, ").
            append("10   as DECIMAL_DIGITS, ").
            append("10   as NUM_PREC_RADIX, ").
            append("colnullable as NULLABLE, ").
            append("null as REMARKS, ").
            append("null as COLUMN_DEF, ").
            append("0    as SQL_DATA_TYPE, ").
            append("0    as SQL_DATETIME_SUB, ").
            append("2000000000 as CHAR_OCTET_LENGTH, ").
            append("ordpos as ORDINAL_POSITION, ").
            append("(case colnullable when 0 then 'N' when 1 then 'Y' else '' end)").
            append("    as IS_NULLABLE, ").
            append("null as SCOPE_CATLOG, ").
            append("null as SCOPE_SCHEMA, ").
            append("null as SCOPE_TABLE, ").
            append("null as SOURCE_DATA_TYPE from (");

        // the command "pragma table_info('tablename')" does not embed
        // like a normal select statement so we must extract the information
        // and then build a resultset from unioned select statements
        rs = stat.executeQuery("pragma table_info ("+escape(tbl)+");");

        boolean colFound = false;
        for (int i=0; rs.next(); i++) {
            String colName = rs.getString(2);
            String colType = rs.getString(3);
            String colNotNull = rs.getString(4);

            int colNullable = 2;
            if (colNotNull != null) colNullable = colNotNull.equals("0") ? 1:0;
            if (colFound) sql.append(" union all ");
            colFound = true;

            colType = getSQLiteType(colType);
            int colJavaType = getJavaType(colType);

            sql.append("select ").
                append(i).append(" as ordpos, ").
                append(colNullable).append(" as colnullable, '").
                append(colJavaType).append("' as ct, ").
                append(escape(colName)).append(" as cn, ").
                append(escape(colType)).append(" as tn");

            if (colPat != null)
                sql.append(" where upper(cn) like upper(").append(escape(colPat)).append(")");
        }
        sql.append(colFound ? ") order by TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;" :
            "select null as ordpos, null as colnullable, "
            + "null as cn, null as tn) limit 0;");
        rs.close();

        return stat.executeQuery(sql.toString());
    }

    private String getSQLiteType(String colType) {
        return colType == null ? "TEXT" : colType.toUpperCase();
    }

    private static int getJavaType(String colType) {
        final int colJavaType;
        if ("INT".equals(colType) || "INTEGER".equals(colType))
            colJavaType = Types.INTEGER;
        else if ("TEXT".equals(colType))
            colJavaType = Types.VARCHAR;
        else if ("FLOAT".equals(colType))
            colJavaType = Types.FLOAT;
        else
            colJavaType = Types.VARCHAR;
        return colJavaType;
    }

    public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
				String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return getForeignKeys(primaryTable, foreignTable, true);
    }

    private ResultSet getForeignKeys(String primaryTable, String foreignTable, boolean cross) throws SQLException {
        Statement stat = conn.createStatement();

        final StringBuilder sql = new StringBuilder();

        sql.append("select ").
            append("null as PKTABLE_CAT, ").
            append("null as PKTABLE_SCHEM, ").
            append("pt as PKTABLE_NAME, ").
            append("pc as PKCOLUMN_NAME, ").
            append("null as FKTABLE_CAT, ").
            append("null as FKTABLE_SCHEM, ").
            append(null == foreignTable ? "null" : escape(foreignTable)).append(" as FKTABLE_NAME, ").
            append("fc as FKCOLUMN_NAME, ").
            append("seq as KEY_SEQ, ").
            append(importedKeyNoAction).append(" as UPDATE_RULE, ").
            append(importedKeyNoAction).append(" as DELETE_RULE, ").
            append("null as FK_NAME, ").
            append("null as PK_NAME, ").
            append(importedKeyNotDeferrable).append(" as DEFERRABILITY ").
            append("from (");

        int i = 0;
        if (null != foreignTable) {
            ResultSet rs = null;
            try {
                rs = stat.executeQuery("pragma foreign_key_list("+escape(foreignTable)+");");
                for (i=0; rs.next(); i++) {
                    final String pt = rs.getString(3);
                    if (null != primaryTable && !primaryTable.equalsIgnoreCase(pt)) { i--; continue; }
                    if (i > 0) {
                        sql.append(" union all ");
                    }
                    sql.append("select ").
                        append(escape(pt)).append(" as pt, ").
                        append(escape(rs.getString(4))).append(" as pc, ").
                        append(escape(rs.getString(5))).append(" as fc, ").
                        append(rs.getShort(2)).append(" as seq");
                }
            } catch(SQLException e) {
                i = 0;
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

        if (i == 0) {
            sql.append("select null as pt, null as pc, null as fc, null as seq) limit 0;");
        } else {
            if (cross) {
                sql.append(") order by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ;");
            } else {
                sql.append(") order by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ;");
            }
        }

        return stat.executeQuery(sql.toString());
    }

    public ResultSet getSchemas() throws SQLException {
        if (getSchemas == null) getSchemas = conn.prepareStatement("select "
                + "null as TABLE_SCHEM, "
                + "null as TABLE_CATALOG "
                + "limit 0;");
        getSchemas.clearParameters();
        return getSchemas.executeQuery();
    }

    public ResultSet getCatalogs() throws SQLException {
        if (getCatalogs == null) getCatalogs = conn.prepareStatement(
                "select null as TABLE_CAT limit 0;");
        getCatalogs.clearParameters();
        return getCatalogs.executeQuery();
    }

    public ResultSet getPrimaryKeys(String c, String s, String table)
            throws SQLException {
        final StringBuffer sql = new StringBuffer();
        Statement stat = conn.createStatement();

        sql.append("select ").
            append("null as TABLE_CAT, ").
            append("null as TABLE_SCHEM, ").
            append(null == table ? "null" : escape(table)).append(" as TABLE_NAME, ").
            append("cn as COLUMN_NAME, ").
            append("seqno as KEY_SEQ, ").
            append("null as PK_NAME from (");

        int i = 0;
        String colName = null;
        ResultSet rs = null;
        if (null != table) {
            try {
                rs = stat.executeQuery("pragma table_info("+escape(table)+");");
                for (i=0; rs.next(); i++) {
                    if (!rs.getBoolean(6)) { i--; continue; }
                    if (i > 1) { break; }
                    colName = rs.getString(2);
                }
            } catch (SQLException e) {
                i = 0;
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
        if (i == 0) {
            sql.append("select null as cn, null as seqno) limit 0;");
        } else if (i == 1) {
            sql.append("select ").
                append(escape(colName)).append(" as cn, ").
                append(0).append(" as seqno) order by COLUMN_NAME;");
        } else {
            String indexName = null;
            try {
                rs = stat.executeQuery("pragma index_list("+escape(table)+");");
                while (rs.next()) {
                    if (!rs.getBoolean(3)) { continue; }
                    final String name = rs.getString(2);
                    if (name.startsWith("sqlite_autoindex_")) { indexName = name; break; }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
            if (null != indexName) {
                try {
                    rs = stat.executeQuery("pragma index_info("+escape(indexName)+");");
                    for (i=0; rs.next(); i++) {
                        if (i > 0) {
                            sql.append(" union all ");
                        }
                        sql.append("select ").
                            append(escape(rs.getString(3))).append(" as cn, ").
                            append(rs.getInt(1)).append(" as seqno");
                    }
                    if (i == 0) {
                        sql.append("select null as cn, null as seqno) limit 0;");
                    } else {
                        sql.append(") order by COLUMN_NAME;");
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            } else {
                sql.append("select null as cn, null as seqno) limit 0;");
            }
        }

        return stat.executeQuery(sql.toString());
    }

    public ResultSet getExportedKeys(String c, String s, String t)
            throws SQLException {
        if (getExportedKeys == null) getExportedKeys = conn.prepareStatement(
                "select "
                + "null as PKTABLE_CAT, "
                + "null as PKTABLE_SCHEM, "
                + "null as PKTABLE_NAME, "
                + "null as PKCOLUMN_NAME, "
                + "null as FKTABLE_CAT, "
                + "null as FKTABLE_SCHEM, "
                + "null as FKTABLE_NAME, "
                + "null as FKCOLUMN_NAME, "
                + "null as KEY_SEQ, "
                + "null as UPDATE_RULE, "
                + "null as DELETE_RULE, "
                + "null as FK_NAME, "
                + "null as PK_NAME, "
                + "null as DEFERRABILITY limit 0;");
        return getExportedKeys.executeQuery();
    }

    public ResultSet getImportedKeys(String catalog, String schema,
			      String table)
            throws SQLException {
        return getForeignKeys(null, table, false);
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table,
			   boolean unique, boolean approximate)
            throws SQLException {
        Statement stat = conn.createStatement();
        final StringBuffer sql = new StringBuffer();
        sql.append("select ").
            append("null as TABLE_CAT, ").
            append("null as TABLE_SCHEM, ").
            append(null == table ? null : escape(table)).append(" as TABLE_NAME, ").
            append("nu as NON_UNIQUE, ").
            append("null as INDEX_QUALIFIER, ").
            append("idx as INDEX_NAME, ").
            append(tableIndexOther).append(" as TYPE, ").
            append("seqno as ORDINAL_POSITION, ").
            append("cn as COLUMN_NAME, ").
            append("'A' as ASC_OR_DESC, ").
            append("0 as CARDINALITY, ").
            append("0 as PAGES, ").
            append("null as FILTER_CONDITION ").
            append("from (");
        
        Map<String,Boolean> indexes = new HashMap<String, Boolean>();
        ResultSet rs = null;
        if (null != table) {
            rs = null;
            try {
                rs = stat.executeQuery("pragma index_list("+escape(table)+");");
                while (rs.next()) {
                    final boolean notuniq = !rs.getBoolean(3);
                    if (unique && notuniq) { continue; }
                    indexes.put(rs.getString(2), notuniq);
                }
            } catch (SQLException e) {
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

        if (indexes.isEmpty()) {
            sql.append("select null as nu, null as idx, null as seqno, null as cn) limit 0;");
        } else {
            boolean found = false;
            for (final Map.Entry<String,Boolean> index : indexes.entrySet()) {
                try {
                    rs = stat.executeQuery("pragma index_info(" + escape(index.getKey()) + ");");
                    while (rs.next()) {
                        if (found) {
                            sql.append(" union all ");
                        }
                        sql.append("select ").
                                append(index.getValue() ? 1 : 0).append(" as nu, ").
                                append(escape(index.getKey())).append(" as idx, ").
                                append(rs.getInt(1)).append(" as seqno, ").
                                append(escape(rs.getString(3))).append(" as cn");
                        found = true;
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            }
            if (found) {
                sql.append(") order by NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION;");
            } else {
                sql.append("select null as nu, null as idx, null as seqno, null as cn) limit 0;");
            }
        }

        return stat.executeQuery(sql.toString());
    }

    public ResultSet getProcedureColumns(String c, String s, String p,
                                         String colPat)
            throws SQLException {
        if (getProcedures == null) getProcedureColumns = conn.prepareStatement(
            "select "
            + "null as PROCEDURE_CAT, "
            + "null as PROCEDURE_SCHEM, "
            + "null as PROCEDURE_NAME, "
            + "null as COLUMN_NAME, "
            + "null as COLUMN_TYPE, "
            + "null as DATA_TYPE, "
            + "null as TYPE_NAME, "
            + "null as PRECISION, "
            + "null as LENGTH, "
            + "null as SCALE, "
            + "null as RADIX, "
            + "null as NULLABLE, "
            + "null as REMARKS limit 0;");
        return getProcedureColumns.executeQuery();

    }

    public ResultSet getProcedures(String c, String s, String p)
            throws SQLException {
        if (getProcedures == null) getProcedures = conn.prepareStatement(
            "select "
            + "null as PROCEDURE_CAT, "
            + "null as PROCEDURE_SCHEM, "
            + "null as PROCEDURE_NAME, "
            + "null as UNDEF1, "
            + "null as UNDEF2, "
            + "null as UNDEF3, "
            + "null as REMARKS, "
            + "null as PROCEDURE_TYPE limit 0;");
        return getProcedures.executeQuery();
    }

    public ResultSet getSuperTables(String c, String s, String t)
            throws SQLException {
        if (getSuperTables == null) getSuperTables = conn.prepareStatement(
            "select "
            + "null as TABLE_CAT, "
            + "null as TABLE_SCHEM, "
            + "null as TABLE_NAME, "
            + "null as SUPERTABLE_NAME limit 0;");
        return getSuperTables.executeQuery();
    }

    public ResultSet getSuperTypes(String c, String s, String t)
            throws SQLException {
        if (getSuperTypes == null) getSuperTypes = conn.prepareStatement(
            "select "
            + "null as TYPE_CAT, "
            + "null as TYPE_SCHEM, "
            + "null as TYPE_NAME, "
            + "null as SUPERTYPE_CAT, "
            + "null as SUPERTYPE_SCHEM, "
            + "null as SUPERTYPE_NAME limit 0;");
        return getSuperTypes.executeQuery();
    }

    public ResultSet getTablePrivileges(String c, String s, String t)
            throws SQLException {
        if (getTablePrivileges == null)
            getTablePrivileges = conn.prepareStatement(
            "select "
            + "null as TABLE_CAT, "
            + "null as TABLE_SCHEM, "
            + "null as TABLE_NAME, "
            + "null as GRANTOR, "
            + "null as GRANTEE, "
            + "null as PRIVILEGE, "
            + "null as IS_GRANTABLE limit 0;");
        return getTablePrivileges.executeQuery();
    }

    public synchronized ResultSet getTables(String c, String s,
            String t, String[] types) throws SQLException {
        checkOpen();

        t = (t == null || "".equals(t)) ? "%" : t;

        final StringBuilder sql = new StringBuilder().append("select").
                append(" null as TABLE_CAT,").
                append(" null as TABLE_SCHEM,").
                append(" name as TABLE_NAME,").
                append(" upper(type) as TABLE_TYPE,").
                append(" null as REMARKS,").
                append(" null as TYPE_CAT,").
                append(" null as TYPE_SCHEM,").
                append(" null as TYPE_NAME,").
                append(" null as SELF_REFERENCING_COL_NAME,").
                append(" null as REF_GENERATION").
                append(" from (select name, type from sqlite_master union all").
                append("       select name, type from sqlite_temp_master)").
                append(" where TABLE_NAME like ").append(escape(t));

        if (types != null) {
            sql.append(" and TABLE_TYPE in (");
            for (int i=0; i < types.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append("'").append(types[i].toUpperCase()).append("'");
            }
            sql.append(")");
        } else {
            sql.append(" and TABLE_TYPE in ('TABLE', 'VIEW')");
        }

        sql.append(" order by TABLE_TYPE, TABLE_SCHEM, TABLE_NAME;");

        return conn.createStatement().executeQuery(sql.toString());
    }

    public ResultSet getTableTypes() throws SQLException {
        checkOpen();
        if (getTableTypes == null) getTableTypes = conn.prepareStatement(
                "select 'TABLE' as TABLE_TYPE"
                + " union select 'VIEW' as TABLE_TYPE;");
        getTableTypes.clearParameters();
        return getTableTypes.executeQuery();
    }

    public ResultSet getTypeInfo() throws SQLException {
        if (getTypeInfo == null) {
            getTypeInfo = conn.prepareStatement(
                  "select "
                + "tn as TYPE_NAME, "
                + "dt as DATA_TYPE, "
                + "0 as PRECISION, "
                + "null as LITERAL_PREFIX, "
                + "null as LITERAL_SUFFIX, "
                + "null as CREATE_PARAMS, "
                + typeNullable + " as NULLABLE, "
                + "1 as CASE_SENSITIVE, "
                + typeSearchable + " as SEARCHABLE, "
                + "0 as UNSIGNED_ATTRIBUTE, "
                + "0 as FIXED_PREC_SCALE, "
                + "0 as AUTO_INCREMENT, "
                + "null as LOCAL_TYPE_NAME, "
                + "0 as MINIMUM_SCALE, "
                + "0 as MAXIMUM_SCALE, "
                + "0 as SQL_DATA_TYPE, "
                + "0 as SQL_DATETIME_SUB, "
                + "10 as NUM_PREC_RADIX from ("
                + "    select 'BLOB' as tn, " + Types.BLOB + " as dt union"
                + "    select 'NULL' as tn, " + Types.NULL + " as dt union"
                + "    select 'REAL' as tn, " + Types.REAL+ " as dt union"
                + "    select 'TEXT' as tn, " + Types.VARCHAR + " as dt union"
                + "    select 'INTEGER' as tn, "+ Types.INTEGER +" as dt"
                + ") order by DATA_TYPE, TYPE_NAME;"
            );
        }

        getTypeInfo.clearParameters();
        return getTypeInfo.executeQuery();
    }

    public ResultSet getUDTs(String c, String s, String t, int[] types)
            throws SQLException {
        if (getUDTs == null) getUDTs = conn.prepareStatement("select "
                + "null as TYPE_CAT, "
                + "null as TYPE_SCHEM, "
                + "null as TYPE_NAME, "
                + "null as CLASS_NAME, "
                + "null as DATA_TYPE, "
                + "null as REMARKS, "
                + "null as BASE_TYPE "
                + "limit 0;");

        getUDTs.clearParameters();
        return getUDTs.executeQuery();
    }
    public ResultSet getVersionColumns(String c, String s, String t)
            throws SQLException {
        if (getVersionColumns == null)
            getVersionColumns = conn.prepareStatement(
            "select "
            + "null as SCOPE, "
            + "null as COLUMN_NAME, "
            + "null as DATA_TYPE, "
            + "null as TYPE_NAME, "
            + "null as COLUMN_SIZE, "
            + "null as BUFFER_LENGTH, "
            + "null as DECIMAL_DIGITS, "
            + "null as PSEUDO_COLUMN limit 0;");
        return getVersionColumns.executeQuery();
    }

    ResultSet getGeneratedKeys() throws SQLException {
        if (getGeneratedKeys == null) getGeneratedKeys = conn.prepareStatement(
            "select last_insert_rowid();");
        return getGeneratedKeys.executeQuery();
    }

    /** Replace all instances of ' with '' */
    private String escape(final String val) {
        // TODO: this function is ugly, pass this work off to SQLite, then we
        //       don't have to worry about Unicode 4, other characters needing
        //       escaping, etc.
        int len = val.length();
        StringBuffer buf = new StringBuffer(len);
        buf.append('\'');
        for (int i=0; i < len; i++) {
            if (val.charAt(i) == '\'') buf.append('\'');
            buf.append(val.charAt(i));
        }
        buf.append('\'');
        return buf.toString();
    }
}
