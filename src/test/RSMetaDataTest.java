package test;

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.*;

public class RSMetaDataTest
{
    private Connection conn;
    private Statement stat;
    private ResultSetMetaData meta;

    @BeforeClass public static void forName() throws Exception {
        Class.forName("org.sqlite.JDBC");
    }

    @Before public void connect() throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:");
        stat = conn.createStatement();
        stat.executeUpdate(
            "create table People (pid integer primary key autoincrement, "
            + " firstname string, surname string, dob date);");
        stat.executeUpdate(
            "insert into people values (null, 'Mohandas', 'Gandhi', "
            + " '1869-10-02');");
        meta = stat.executeQuery(
            "select pid, firstname, surname from people;").getMetaData();
    }

    @After public void close() throws SQLException {
        stat.executeUpdate("drop table people;");
        stat.close();
        conn.close();
    }

    @Test public void catalogName() throws SQLException {
        assertEquals("People", meta.getCatalogName(1));
    }

    @Test public void columns() throws SQLException {
        assertEquals(3, meta.getColumnCount());
        assertEquals("pid", meta.getColumnName(1));
        assertEquals("firstname", meta.getColumnName(2));
        assertEquals("surname", meta.getColumnName(3));
        assertEquals(Types.INTEGER, meta.getColumnType(1));
        assertEquals(Types.VARCHAR, meta.getColumnType(2));
        assertEquals(Types.VARCHAR, meta.getColumnType(3));
        assertEquals("integer", meta.getColumnTypeName(1));
        assertEquals("text", meta.getColumnTypeName(2));
        assertEquals("text", meta.getColumnTypeName(3));
        assertTrue(meta.isAutoIncrement(1));
        assertFalse(meta.isAutoIncrement(2));
        assertFalse(meta.isAutoIncrement(3));
        assertEquals(meta.columnNoNulls, meta.isNullable(1));
        assertEquals(meta.columnNullable, meta.isNullable(2));
        assertEquals(meta.columnNullable, meta.isNullable(3));
    }

    @Test public void differentRS() throws SQLException {
        meta = stat.executeQuery("select * from people;").getMetaData();
        assertEquals(4, meta.getColumnCount());
        assertEquals("pid", meta.getColumnName(1));
        assertEquals("firstname", meta.getColumnName(2));
        assertEquals("surname", meta.getColumnName(3));
        assertEquals("dob", meta.getColumnName(4));
    }

    @Test public void nullable() throws SQLException {
        meta = stat.executeQuery("select null;").getMetaData();
        assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(1));
    }

    @Test(expected= SQLException.class)
    public void badCatalogIndex() throws SQLException { meta.getCatalogName(4);}

    @Test(expected= SQLException.class)
    public void badColumnIndex() throws SQLException { meta.getColumnName(4); }

}
