package test;

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.*;

/** These tests are designed to stress Statements on memory databases. */
public class DBMetaDataTest
{
    private Connection conn;
    private Statement stat;
    private DatabaseMetaData meta;

    @BeforeClass public static void forName() throws Exception {
        Class.forName("org.sqlite.JDBC");
    }

    @Before public void connect() throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:");
        stat = conn.createStatement();
        stat.executeUpdate(
            "create table test (id integer primary key, fn, sn);");
        stat.executeUpdate("create view testView as select * from test;");
        meta = conn.getMetaData();
    }

    @After public void close() throws SQLException {
        meta = null;
        stat.close();
        conn.close();
    }

    @Test public void getTables() throws SQLException {
        ResultSet rs = meta.getTables(null, null, null, null);
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("test", rs.getString("TABLE_NAME")); // 3
        assertEquals("TABLE", rs.getString("TABLE_TYPE")); // 4
        assertTrue(rs.next());
        assertEquals("testView", rs.getString("TABLE_NAME"));
        assertEquals("VIEW", rs.getString("TABLE_TYPE"));
        rs.close();

        rs = meta.getTables(null, null, "bob", null);
        assertFalse(rs.next());
        rs.close();
        rs = meta.getTables(null, null, "test", null);
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
        rs = meta.getTables(null, null, "test%", null);
        assertTrue(rs.next());
        assertTrue(rs.next());
        rs.close();

        rs = meta.getTables(null, null, null, new String[] { "table" });
        assertTrue(rs.next());
        assertEquals("test", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs.close();

        rs = meta.getTables(null, null, null, new String[] { "view" });
        assertTrue(rs.next());
        assertEquals("testView", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void getTableTypes() throws SQLException {
        ResultSet rs = meta.getTableTypes();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertTrue(rs.next());
        assertEquals("VIEW", rs.getString("TABLE_TYPE"));
        assertFalse(rs.next());
    }

    @Test public void getTypeInfo() throws SQLException {
        ResultSet rs = meta.getTypeInfo();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("NULL", rs.getString("TYPE_NAME"));
        assertTrue(rs.next());
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertTrue(rs.next());
        assertEquals("REAL", rs.getString("TYPE_NAME"));
        assertTrue(rs.next());
        assertEquals("TEXT", rs.getString("TYPE_NAME"));
        assertTrue(rs.next());
        assertEquals("BLOB", rs.getString("TYPE_NAME"));
        assertFalse(rs.next());
    }

    @Test public void getColumns() throws SQLException {
        ResultSet rs = meta.getColumns(null, null, "test", "id");
        assertTrue(rs.next());
        assertEquals("test", rs.getString("TABLE_NAME"));
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "test", "fn");
        assertTrue(rs.next());
        assertEquals("fn", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "test", "sn");
        assertTrue(rs.next());
        assertEquals("sn", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "test", "%");
        assertTrue(rs.next());
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("fn", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("sn", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "test", "%n");
        assertTrue(rs.next());
        assertEquals("fn", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("sn", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "test%", "%");
        assertTrue(rs.next());
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("fn", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("sn", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "%", "%");
        assertTrue(rs.next());
        assertEquals("test", rs.getString("TABLE_NAME"));
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("fn", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("sn", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        rs = meta.getColumns(null, null, "doesnotexist", "%");
        assertFalse(rs.next());
    }

    @Test public void columnOrderOfgetTables() throws SQLException {
        ResultSet rs = meta.getTables(null, null, null, null);
        assertTrue(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(10, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("TABLE_TYPE", rsmeta.getColumnName(4));
        assertEquals("REMARKS", rsmeta.getColumnName(5));
        assertEquals("TYPE_CAT", rsmeta.getColumnName(6));
        assertEquals("TYPE_SCHEM", rsmeta.getColumnName(7));
        assertEquals("TYPE_NAME", rsmeta.getColumnName(8));
        assertEquals("SELF_REFERENCING_COL_NAME", rsmeta.getColumnName(9));
        assertEquals("REF_GENERATION", rsmeta.getColumnName(10));
    }

    @Test public void columnOrderOfgetTableTypes() throws SQLException {
        ResultSet rs = meta.getTableTypes();
        assertTrue(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(1, rsmeta.getColumnCount());
        assertEquals("TABLE_TYPE", rsmeta.getColumnName(1));
    }

    @Test public void columnOrderOfgetTypeInfo() throws SQLException {
        ResultSet rs = meta.getTypeInfo();
        assertTrue(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(18, rsmeta.getColumnCount());
        assertEquals("TYPE_NAME", rsmeta.getColumnName(1));
        assertEquals("DATA_TYPE", rsmeta.getColumnName(2));
        assertEquals("PRECISION", rsmeta.getColumnName(3));
        assertEquals("LITERAL_PREFIX", rsmeta.getColumnName(4));
        assertEquals("LITERAL_SUFFIX", rsmeta.getColumnName(5));
        assertEquals("CREATE_PARAMS", rsmeta.getColumnName(6));
        assertEquals("NULLABLE", rsmeta.getColumnName(7));
        assertEquals("CASE_SENSITIVE", rsmeta.getColumnName(8));
        assertEquals("SEARCHABLE", rsmeta.getColumnName(9));
        assertEquals("UNSIGNED_ATTRIBUTE", rsmeta.getColumnName(10));
        assertEquals("FIXED_PREC_SCALE", rsmeta.getColumnName(11));
        assertEquals("AUTO_INCREMENT", rsmeta.getColumnName(12));
        assertEquals("LOCAL_TYPE_NAME", rsmeta.getColumnName(13));
        assertEquals("MINIMUM_SCALE", rsmeta.getColumnName(14));
        assertEquals("MAXIMUM_SCALE", rsmeta.getColumnName(15));
        assertEquals("SQL_DATA_TYPE", rsmeta.getColumnName(16));
        assertEquals("SQL_DATETIME_SUB", rsmeta.getColumnName(17));
        assertEquals("NUM_PREC_RADIX", rsmeta.getColumnName(18));
    }

    @Test public void columnOrderOfgetColumns() throws SQLException {
        ResultSet rs = meta.getColumns(null, null, "test", null);
        assertTrue(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(22, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(4));
        assertEquals("DATA_TYPE", rsmeta.getColumnName(5));
        assertEquals("TYPE_NAME", rsmeta.getColumnName(6));
        assertEquals("COLUMN_SIZE", rsmeta.getColumnName(7));
        assertEquals("BUFFER_LENGTH", rsmeta.getColumnName(8));
        assertEquals("DECIMAL_DIGITS", rsmeta.getColumnName(9));
        assertEquals("NUM_PREC_RADIX", rsmeta.getColumnName(10));
        assertEquals("NULLABLE", rsmeta.getColumnName(11));
        assertEquals("REMARKS", rsmeta.getColumnName(12));
        assertEquals("COLUMN_DEF", rsmeta.getColumnName(13));
        assertEquals("SQL_DATA_TYPE", rsmeta.getColumnName(14));
        assertEquals("SQL_DATETIME_SUB", rsmeta.getColumnName(15));
        assertEquals("CHAR_OCTET_LENGTH", rsmeta.getColumnName(16));
        assertEquals("ORDINAL_POSITION", rsmeta.getColumnName(17));
        assertEquals("IS_NULLABLE", rsmeta.getColumnName(18));
        // should be SCOPE_CATALOG, but misspelt in the standard
        assertEquals("SCOPE_CATLOG", rsmeta.getColumnName(19));
        assertEquals("SCOPE_SCHEMA", rsmeta.getColumnName(20));
        assertEquals("SCOPE_TABLE", rsmeta.getColumnName(21));
        assertEquals("SOURCE_DATA_TYPE", rsmeta.getColumnName(22));
    }

    // the following functions always return an empty resultset, so
    // do not bother testing their parameters, only the column types

    @Test public void columnOrderOfgetProcedures() throws SQLException {
        ResultSet rs = meta.getProcedures(null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(8, rsmeta.getColumnCount());
        assertEquals("PROCEDURE_CAT", rsmeta.getColumnName(1));
        assertEquals("PROCEDURE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("PROCEDURE_NAME", rsmeta.getColumnName(3));
        // currently (Java 1.5), cols 4,5,6 are undefined
        assertEquals("REMARKS", rsmeta.getColumnName(7));
        assertEquals("PROCEDURE_TYPE", rsmeta.getColumnName(8));
    }

    @Test public void columnOrderOfgetProcedurColumns() throws SQLException {
        ResultSet rs = meta.getProcedureColumns(null, null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(13, rsmeta.getColumnCount());
        assertEquals("PROCEDURE_CAT", rsmeta.getColumnName(1));
        assertEquals("PROCEDURE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("PROCEDURE_NAME", rsmeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(4));
        assertEquals("COLUMN_TYPE", rsmeta.getColumnName(5));
        assertEquals("DATA_TYPE", rsmeta.getColumnName(6));
        assertEquals("TYPE_NAME", rsmeta.getColumnName(7));
        assertEquals("PRECISION", rsmeta.getColumnName(8));
        assertEquals("LENGTH", rsmeta.getColumnName(9));
        assertEquals("SCALE", rsmeta.getColumnName(10));
        assertEquals("RADIX", rsmeta.getColumnName(11));
        assertEquals("NULLABLE", rsmeta.getColumnName(12));
        assertEquals("REMARKS", rsmeta.getColumnName(13));
    }

    @Test public void columnOrderOfgetSchemas() throws SQLException {
        ResultSet rs = meta.getSchemas();
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(2, rsmeta.getColumnCount());
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(1));
        assertEquals("TABLE_CATALOG", rsmeta.getColumnName(2));
    }

    @Test public void columnOrderOfgetCatalogs() throws SQLException {
        ResultSet rs = meta.getCatalogs();
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(1, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
    }

    @Test public void columnOrderOfgetColumnPrivileges() throws SQLException {
        ResultSet rs = meta.getColumnPrivileges(null, null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(8, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(4));
        assertEquals("GRANTOR", rsmeta.getColumnName(5));
        assertEquals("GRANTEE", rsmeta.getColumnName(6));
        assertEquals("PRIVILEGE", rsmeta.getColumnName(7));
        assertEquals("IS_GRANTABLE", rsmeta.getColumnName(8));
    }

    @Test public void columnOrderOfgetTablePrivileges() throws SQLException {
        ResultSet rs = meta.getTablePrivileges(null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(7, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("GRANTOR", rsmeta.getColumnName(4));
        assertEquals("GRANTEE", rsmeta.getColumnName(5));
        assertEquals("PRIVILEGE", rsmeta.getColumnName(6));
        assertEquals("IS_GRANTABLE", rsmeta.getColumnName(7));
    }

    @Test public void columnOrderOfgetBestRowIdentifier() throws SQLException {
        ResultSet rs = meta.getBestRowIdentifier(null, null, null, 0, false);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(8, rsmeta.getColumnCount());
        assertEquals("SCOPE", rsmeta.getColumnName(1));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(2));
        assertEquals("DATA_TYPE", rsmeta.getColumnName(3));
        assertEquals("TYPE_NAME", rsmeta.getColumnName(4));
        assertEquals("COLUMN_SIZE", rsmeta.getColumnName(5));
        assertEquals("BUFFER_LENGTH", rsmeta.getColumnName(6));
        assertEquals("DECIMAL_DIGITS", rsmeta.getColumnName(7));
        assertEquals("PSEUDO_COLUMN", rsmeta.getColumnName(8));
        rs = meta.getBestRowIdentifier(null, null, "test", 0, false);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals("ROWID", rs.getString(2));
        assertEquals(Types.INTEGER, rs.getInt(3));
        assertEquals("INTEGER", rs.getString(4));
        assertEquals(10, rs.getInt(5));
        assertEquals(0, rs.getInt(6));
        assertEquals(0, rs.getInt(7));
        assertEquals(DatabaseMetaData.bestRowPseudo, rs.getInt(8));
    }

    @Test public void columnOrderOfgetVersionColumns() throws SQLException {
        ResultSet rs = meta.getVersionColumns(null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(8, rsmeta.getColumnCount());
        assertEquals("SCOPE", rsmeta.getColumnName(1));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(2));
        assertEquals("DATA_TYPE", rsmeta.getColumnName(3));
        assertEquals("TYPE_NAME", rsmeta.getColumnName(4));
        assertEquals("COLUMN_SIZE", rsmeta.getColumnName(5));
        assertEquals("BUFFER_LENGTH", rsmeta.getColumnName(6));
        assertEquals("DECIMAL_DIGITS", rsmeta.getColumnName(7));
        assertEquals("PSEUDO_COLUMN", rsmeta.getColumnName(8));
    }

    @Test public void columnOrderOfgetPrimaryKeys() throws SQLException {
        ResultSet rs;
        ResultSetMetaData rsmeta;

        stat.executeUpdate("create table nopk (c1, c2, c3, c4);");
        stat.executeUpdate("create table pk1 (col1 primary key, col2, col3);");
        stat.executeUpdate("create table pk2 (col1, col2 primary key, col3);");
        stat.executeUpdate("create table pk3 (col1, col2, col3, col4, "
                + "primary key (col2, col3));");

        rs = meta.getPrimaryKeys(null, null, "nopk");
        assertFalse(rs.next());
        rsmeta = rs.getMetaData();
        assertEquals(6, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(4));
        assertEquals("KEY_SEQ", rsmeta.getColumnName(5));
        assertEquals("PK_NAME", rsmeta.getColumnName(6));
        rs.close();

        rs = meta.getPrimaryKeys(null, null, "pk1");
        assertTrue(rs.next());
        assertEquals("col1", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs.close();

        rs = meta.getPrimaryKeys(null, null, "pk2");
        assertTrue(rs.next());
        assertEquals("col2", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs.close();

        rs = meta.getPrimaryKeys(null, null, "pk3");
        assertTrue(rs.next());
        assertEquals("col2", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("col3", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs.close();
    }

    /* TODO
    @Test public void columnOrderOfgetImportedKeys() throws SQLException {
    @Test public void columnOrderOfgetExportedKeys() throws SQLException {
    @Test public void columnOrderOfgetCrossReference() throws SQLException {
    @Test public void columnOrderOfgetTypeInfo() throws SQLException {
    @Test public void columnOrderOfgetIndexInfo() throws SQLException {
    @Test public void columnOrderOfgetSuperTypes() throws SQLException {
    @Test public void columnOrderOfgetSuperTables() throws SQLException {
    @Test public void columnOrderOfgetAttributes() throws SQLException {*/

    @Test public void columnOrderOfgetUDTs() throws SQLException {
        ResultSet rs = meta.getUDTs(null, null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(7, rsmeta.getColumnCount());
        assertEquals("TYPE_CAT", rsmeta.getColumnName(1));
        assertEquals("TYPE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TYPE_NAME", rsmeta.getColumnName(3));
        assertEquals("CLASS_NAME", rsmeta.getColumnName(4));
        assertEquals("DATA_TYPE", rsmeta.getColumnName(5));
        assertEquals("REMARKS", rsmeta.getColumnName(6));
        assertEquals("BASE_TYPE", rsmeta.getColumnName(7));
    }

    @Test public void version() throws SQLException {
        assertNotNull(meta.getDatabaseProductVersion());
        assertTrue(
            "pure".equals(meta.getDriverVersion()) ||
            "native".equals(meta.getDriverVersion())
        );
    }

    @Test public void indexInfo() throws SQLException {
        ResultSet rs = meta.getIndexInfo(null, null, null, false, false);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(13, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("NON_UNIQUE", rsmeta.getColumnName(4));
        assertEquals("INDEX_QUALIFIER", rsmeta.getColumnName(5));
        assertEquals("INDEX_NAME", rsmeta.getColumnName(6));
        assertEquals("TYPE", rsmeta.getColumnName(7));
        assertEquals("ORDINAL_POSITION", rsmeta.getColumnName(8));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(9));
        assertEquals("ASC_OR_DESC", rsmeta.getColumnName(10));
        assertEquals("CARDINALITY", rsmeta.getColumnName(11));
        assertEquals("PAGES", rsmeta.getColumnName(12));
        assertEquals("FILTER_CONDITION", rsmeta.getColumnName(13));

        rs = meta.getIndexInfo(null, null, "test", true, false);
        assertFalse(rs.next());
    }

    @Test public void primaryKeys() throws SQLException {
        ResultSet rs = meta.getPrimaryKeys(null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(6, rsmeta.getColumnCount());
        assertEquals("TABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("TABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("TABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmeta.getColumnName(4));
        assertEquals("KEY_SEQ", rsmeta.getColumnName(5));
        assertEquals("PK_NAME", rsmeta.getColumnName(6));

        rs = meta.getPrimaryKeys(null, null, "test");
        assertTrue(rs.next());
        assertEquals("test", rs.getString(3));
        assertEquals("id", rs.getString(4));
        assertEquals(0, rs.getInt(5));
        assertNull(rs.getString(6));
    }

    @Test public void crossReference() throws SQLException {
        ResultSet rs = meta.getCrossReference(null, null, null, null, null, null);
        assertFalse(rs.next());
        ResultSetMetaData rsmeta = rs.getMetaData();
        assertEquals(14, rsmeta.getColumnCount());
        assertEquals("PKTABLE_CAT", rsmeta.getColumnName(1));
        assertEquals("PKTABLE_SCHEM", rsmeta.getColumnName(2));
        assertEquals("PKTABLE_NAME", rsmeta.getColumnName(3));
        assertEquals("PKCOLUMN_NAME", rsmeta.getColumnName(4));
        assertEquals("FKTABLE_CAT", rsmeta.getColumnName(5));
        assertEquals("FKTABLE_SCHEM", rsmeta.getColumnName(6));
        assertEquals("FKTABLE_NAME", rsmeta.getColumnName(7));
        assertEquals("FKCOLUMN_NAME", rsmeta.getColumnName(8));
        assertEquals("KEY_SEQ", rsmeta.getColumnName(9));
        assertEquals("UPDATE_RULE", rsmeta.getColumnName(10));
        assertEquals("DELETE_RULE", rsmeta.getColumnName(11));
        assertEquals("FK_NAME", rsmeta.getColumnName(12));
        assertEquals("PK_NAME", rsmeta.getColumnName(13));
        assertEquals("DEFERRABILITY", rsmeta.getColumnName(14));
        rs = meta.getCrossReference(null, null, null, null, null, "test");
        assertFalse(rs.next());
    }
}
