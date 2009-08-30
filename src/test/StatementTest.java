package test;

import java.sql.*;
import org.junit.*;
import static org.junit.Assert.*;

/** These tests are designed to stress Statements on memory databases. */
public class StatementTest
{
    private Connection conn;
    private Statement stat;

    @BeforeClass public static void forName() throws Exception {
        Class.forName("org.sqlite.JDBC");
    }

    @Before public void connect() throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:");
        stat = conn.createStatement();
    }

    @After public void close() throws SQLException {
        stat.close();
        conn.close();
    }

    @Test public void stmtUpdate() throws SQLException {
        assertEquals(0, stat.executeUpdate("create table s1 (c1);"));
        assertEquals(1, stat.executeUpdate("insert into s1 values (0);"));
        assertEquals(1, stat.executeUpdate("insert into s1 values (1);"));
        assertEquals(1, stat.executeUpdate("insert into s1 values (2);"));
        ResultSet rs = stat.executeQuery("select count(c1) from s1;");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs.close();
        assertEquals(3, stat.executeUpdate("update s1 set c1 = 5;"));
        assertEquals(3, stat.executeUpdate("delete from s1;"));
        assertEquals(3, stat.executeUpdate("drop table s1;"));
    }

    @Test public void emptyRS() throws SQLException {
        ResultSet rs = stat.executeQuery("select null limit 0;");
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void singleRowRS() throws SQLException {
        ResultSet rs = stat.executeQuery("select " + Integer.MAX_VALUE + ";");
        assertTrue(rs.next());
        assertEquals(Integer.MAX_VALUE, rs.getInt(1));
        assertEquals(Integer.toString(Integer.MAX_VALUE), rs.getString(1));
        assertEquals(new Integer(Integer.MAX_VALUE).doubleValue(),
                rs.getDouble(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void twoRowRS() throws SQLException {
        ResultSet rs = stat.executeQuery("select 9 union all select 7;");
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void autoClose() throws SQLException {
        conn.createStatement().executeQuery("select 1;");
    }

    @Test public void stringRS() throws SQLException {
        ResultSet rs = stat.executeQuery("select \"Russell\";");
        assertTrue(rs.next());
        assertEquals("Russell", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void execute() throws SQLException {
        assertTrue(stat.execute("select null;"));
        ResultSet rs = stat.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertTrue(rs.wasNull());
        assertFalse(stat.getMoreResults());
        assertEquals(-1, stat.getUpdateCount());

        assertTrue(stat.execute("select null;"));
        assertFalse(stat.getMoreResults());
        assertEquals(-1, stat.getUpdateCount());

        assertFalse(stat.execute("create table test (c1);"));
        assertEquals(0, stat.getUpdateCount());
        assertFalse(stat.getMoreResults());
        assertEquals(-1, stat.getUpdateCount());
    }

    @Test public void colNameAccess() throws SQLException {
        assertEquals(stat.executeUpdate(
                    "create table tab (id, firstname, surname);"), 0);
        assertEquals(stat.executeUpdate(
                    "insert into tab values (0, 'Bob', 'Builder');"), 1);
        assertEquals(stat.executeUpdate(
                    "insert into tab values (1, 'Fred', 'Blogs');"), 1);
        assertEquals(stat.executeUpdate(
                    "insert into tab values (2, 'John', 'Smith');"), 1);
        ResultSet rs = stat.executeQuery("select * from tab;");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt("id"));
        assertEquals("Bob", rs.getString("firstname"));
        assertEquals("Builder", rs.getString("surname"));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Fred", rs.getString("firstname"));
        assertEquals("Blogs", rs.getString("surname"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("2",  rs.getString("id"));
        assertEquals("John", rs.getString("firstname"));
        assertEquals("Smith", rs.getString("surname"));
        assertFalse(rs.next());
        rs.close();
        assertEquals(1, stat.executeUpdate("drop table tab;"));
    }

    @Test public void nulls() throws SQLException {
        ResultSet rs = stat.executeQuery("select null union all select null;");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertTrue(rs.wasNull());
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void tempTable() throws SQLException {
        assertEquals(0, stat.executeUpdate("create temp table myTemp (a);"));
        assertEquals(1, stat.executeUpdate("insert into myTemp values (2);"));
    }

    @Test public void insert1000() throws SQLException {
        assertEquals(0, stat.executeUpdate("create table in1000 (a);"));
        conn.setAutoCommit(false);
        for (int i=0; i < 1000; i++)
            assertEquals(stat.executeUpdate(
                "insert into in1000 values ("+i+");"), 1);
        conn.commit();

        ResultSet rs = stat.executeQuery("select count(a) from in1000;");
        assertTrue(rs.next());
        assertEquals(1000, rs.getInt(1));
        rs.close();

        assertEquals(1, stat.executeUpdate("drop table in1000;"));
    }

    private void assertArrayEq(int[] a, int[] b) {
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(a.length, b.length);
        for (int i=0; i < a.length; i++)
            assertEquals(a[i], b[i]);
    }

    @Test public void batch() throws SQLException {
        stat.addBatch("create table batch (c1);");
        stat.addBatch("insert into batch values (1);");
        stat.addBatch("insert into batch values (1);");
        stat.addBatch("insert into batch values (2);");
        stat.addBatch("insert into batch values (3);");
        stat.addBatch("insert into batch values (4);");
        stat.addBatch("insert into batch values (5);");
        stat.addBatch("insert into batch values (6);");
        stat.addBatch("insert into batch values (7);");
        stat.addBatch("insert into batch values (8);");
        stat.addBatch("insert into batch values (9);");
        stat.addBatch("insert into batch values (10);");
        stat.addBatch("insert into batch values (11);");
        stat.addBatch("insert into batch values (12);");
        assertArrayEq(new int[] { 0,1,1,1,1,1,1,1,1,1,1,1,1,1 },
            stat.executeBatch());
        assertArrayEq(new int[] { }, stat.executeBatch());
        stat.clearBatch();
        stat.addBatch("insert into batch values (9);");
        assertArrayEq(new int[] { 1 }, stat.executeBatch());
        assertArrayEq(new int[] {}, stat.executeBatch());
        stat.clearBatch();
        stat.addBatch("insert into batch values (7);");
        stat.addBatch("insert into batch values (7);");
        assertArrayEq(new int[] { 1, 1 }, stat.executeBatch());
        stat.clearBatch();

        ResultSet rs = stat.executeQuery("select count(*) from batch;");
        assertTrue(rs.next());
        assertEquals(16, rs.getInt(1));
        rs.close();
    }

    @Test public void closeOnFalseNext() throws SQLException {
        stat.executeUpdate("create table t1 (c1);");
        conn.createStatement().executeQuery("select * from t1;").next();
        stat.executeUpdate("drop table t1;");
    }

    @Test public void getGeneratedKeys() throws SQLException {
        ResultSet rs;
        stat.executeUpdate("create table t1 (c1 integer primary key, v);");
        stat.executeUpdate("insert into t1 (v) values ('red');");
        rs = stat.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.close();
        stat.executeUpdate("insert into t1 (v) values ('blue');");
        rs = stat.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs.close();
    }

    @Test public void isBeforeFirst() throws SQLException {
        ResultSet rs = stat.executeQuery("select 1 union all select 2;");
        assertTrue(rs.isBeforeFirst());
        assertTrue(rs.next());
        assertTrue(rs.isFirst());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        assertFalse(rs.isBeforeFirst());
        rs.close();
    }

    @Test public void columnNaming() throws SQLException {
        stat.executeUpdate("create table t1 (c1 integer);");
        stat.executeUpdate("create table t2 (c1 integer);");
        stat.executeUpdate("insert into t1 values (1);");
        stat.executeUpdate("insert into t2 values (1);");
        ResultSet rs = stat.executeQuery(
            "select a.c1 AS c1 from t1 a, t2 where a.c1=t2.c1;");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("c1"));
        rs.close();
    }

    @Test public void maxRows() throws SQLException {
        stat.setMaxRows(1);
        assertEquals(1, stat.getMaxRows());
        ResultSet rs = stat.executeQuery("select 1 union select 2;");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void nullDate() throws SQLException {
        ResultSet rs = stat.executeQuery("select null;");
        assertTrue(rs.next());
        assertEquals(null, rs.getDate(1));
        assertEquals(null, rs.getTime(1));
        assertEquals(null, rs.getTimestamp(1));
        rs.close();
    }

    public void ambiguousColumnNaming() throws SQLException {
        stat.executeUpdate("create table t1 (c1 int);");
        stat.executeUpdate("create table t2 (c1 int, c2 int);");
        stat.executeUpdate("insert into t1 values (1);");
        stat.executeUpdate("insert into t2 values (2, 1);");
        ResultSet rs = stat.executeQuery(
            "select a.c1, b.c1 from t1 a, t2 b where a.c1=b.c2;");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("c1"));
        rs.close();
    }

    @Test(expected= SQLException.class)
    public void failToDropWhenRSOpen() throws SQLException {
        stat.executeUpdate("create table t1 (c1);");
        stat.executeUpdate("insert into t1 values (4);");
        stat.executeUpdate("insert into t1 values (4);");
        conn.createStatement().executeQuery("select * from t1;").next();
        stat.executeUpdate("drop table t1;");
    }

    @Test(expected= SQLException.class)
    public void executeNoRS() throws SQLException {
        assertFalse(stat.execute("insert into test values (8);"));
        stat.getResultSet();
    }

    @Test(expected= SQLException.class)
    public void executeClearRS() throws SQLException {
        assertTrue(stat.execute("select null;"));
        assertNotNull(stat.getResultSet());
        assertFalse(stat.getMoreResults());
        stat.getResultSet();
    }

    @Test(expected= BatchUpdateException.class)
    public void batchReturnsResults() throws SQLException {
        stat.addBatch("select null;");
        stat.executeBatch();
    }

    @Test(expected= SQLException.class)
    public void noSuchTable() throws SQLException {
        stat.executeQuery("select * from doesnotexist;");
    }
    
    @Test(expected= SQLException.class)
    public void noSuchCol() throws SQLException {
        stat.executeQuery("select notacol from (select 1);");
    }

    @Test(expected= SQLException.class)
    public void noSuchColName() throws SQLException {
        ResultSet rs = stat.executeQuery("select 1;");
        assertTrue(rs.next());
        rs.getInt("noSuchColName");
    }

    @Test public void queryTimeout() throws SQLException {
        final int timeout = 30;      
        stat.setQueryTimeout(timeout);
        assertEquals(timeout, stat.getQueryTimeout());
    }
}
