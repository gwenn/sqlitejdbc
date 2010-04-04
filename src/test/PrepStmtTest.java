package test;

import java.sql.*;
import java.util.Properties;
import java.util.StringTokenizer;
import org.junit.*;
import static org.junit.Assert.*;

/** These tests are designed to stress PreparedStatements on memory dbs. */
public class PrepStmtTest
{
    static byte[] b1 = new byte[] { 1,2,7,4,2,6,2,8,5,2,3,1,5,3,6,3,3,6,2,5 };
    static byte[] b2 = "To be or not to be.".getBytes();
    static byte[] b3 = "Question!#$%".getBytes();
    static String utf01 = "\uD840\uDC40";
    static String utf02 = "\uD840\uDC47 ";
    static String utf03 = " \uD840\uDC43";
    static String utf04 = " \uD840\uDC42 ";
    static String utf05 = "\uD840\uDC40\uD840\uDC44";
    static String utf06 = "Hello World, \uD840\uDC40 \uD880\uDC99";
    static String utf07 = "\uD840\uDC41 testing \uD880\uDC99";
    static String utf08 = "\uD840\uDC40\uD840\uDC44 testing";

    private Connection conn;
    private Statement stat;

    @BeforeClass public static void forName() throws Exception {
        Class.forName("org.sqlite.JDBC");
    }

    @Before public void connect() throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:");
        stat = conn.createStatement();
    }
    
    private void connectWithJulianDayModeActivated() throws SQLException {
        close();
        final Properties info = new Properties();
        info.setProperty("julian_day", "true");
        conn = DriverManager.getConnection("jdbc:sqlite:", info);
        stat = conn.createStatement();
    }

    @After public void close() throws SQLException {
        stat.close();
        conn.close();
    }

    @Test public void update() throws SQLException {
        assertEquals(conn.prepareStatement(
                "create table s1 (c1);").executeUpdate(), 0);
        PreparedStatement prep = conn.prepareStatement(
                "insert into s1 values (?);");
        prep.setInt(1, 3); assertEquals(1, prep.executeUpdate());
        prep.setInt(1, 5); assertEquals(1, prep.executeUpdate());
        prep.setInt(1, 7); assertEquals(1, prep.executeUpdate());
        prep.close();

        // check results with normal statement
        ResultSet rs = stat.executeQuery("select sum(c1) from s1;");
        assertTrue(rs.next());
        assertEquals(15, rs.getInt(1));
        rs.close();
    }

    @Test public void multiUpdate() throws SQLException {
        stat.executeUpdate("create table test (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values (?);");

        for (int i=0; i < 10; i++) {
            prep.setInt(1, i);
            prep.executeUpdate();
            prep.execute();
        }

        prep.close();
        stat.executeUpdate("drop table test;");
    }

    @Test public void emptyRS() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("select null limit 0;");
        ResultSet rs = prep.executeQuery();
        assertFalse(rs.next());
        rs.close();
        prep.close();
    }

    @Test public void singleRowRS() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("select ?;");
        prep.setInt(1, Integer.MAX_VALUE);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(Integer.MAX_VALUE, rs.getInt(1));
        assertEquals(Integer.toString(Integer.MAX_VALUE), rs.getString(1));
        assertEquals(new Integer(Integer.MAX_VALUE).doubleValue(),
                rs.getDouble(1));
        assertFalse(rs.next());
        rs.close();
        prep.close();
    }

    @Test public void twoRowRS() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
            "select ? union all select ?;");
        prep.setDouble(1, Double.MAX_VALUE);
        prep.setDouble(2, Double.MIN_VALUE);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(Double.MAX_VALUE, rs.getDouble(1));
        assertTrue(rs.next());
        assertEquals(Double.MIN_VALUE, rs.getDouble(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void stringRS() throws SQLException {
        String name = "Gandhi";
        PreparedStatement prep = conn.prepareStatement("select ?;");
        prep.setString(1, name);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(name, rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void finalizePrep() throws SQLException {
        conn.prepareStatement("select null;");
        System.gc();
    }

    @Test public void set() throws SQLException {
        ResultSet rs;
        PreparedStatement prep = conn.prepareStatement("select ?, ?, ?;");

        // integers
        prep.setInt(1, Integer.MIN_VALUE);
        prep.setInt(2, Integer.MAX_VALUE);
        prep.setInt(3, 0);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(Integer.MIN_VALUE, rs.getInt(1));
        assertEquals(Integer.MAX_VALUE, rs.getInt(2));
        assertEquals(0, rs.getInt(3));

        // strings
        String name = "Winston Leonard Churchill";
        String fn = name.substring(0, 7),
               mn = name.substring(8, 15),
               sn = name.substring(16, 25);
        prep.clearParameters();
        prep.setString(1, fn);
        prep.setString(2, mn);
        prep.setString(3, sn);
        prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(fn, rs.getString(1));
        assertEquals(mn, rs.getString(2));
        assertEquals(sn, rs.getString(3));

        // mixed
        prep.setString(1, name);
        prep.setString(2, null);
        prep.setLong(3, Long.MAX_VALUE);
        prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(name, rs.getString(1));
        assertNull(rs.getString(2));
        assertTrue(rs.wasNull());
        assertEquals(Long.MAX_VALUE, rs.getLong(3));

        // bytes
        prep.setBytes(1, b1);
        prep.setBytes(2, b2);
        prep.setBytes(3, b3);
        prep.executeQuery();
        assertTrue(rs.next());
        assertArrayEq(rs.getBytes(1), b1);
        assertArrayEq(rs.getBytes(2), b2);
        assertArrayEq(rs.getBytes(3), b3);
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void colNameAccess() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
            "select ? as col1, ? as col2, ? as bingo;");
        prep.setNull(1, 0);
        prep.setFloat(2, Float.MIN_VALUE);
        prep.setShort(3, Short.MIN_VALUE);
        prep.executeQuery();
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertNull(rs.getString("col1"));
        assertTrue(rs.wasNull());
        assertEquals(Float.MIN_VALUE, rs.getFloat("col2"));
        assertEquals(Short.MIN_VALUE, rs.getShort("bingo"));
        rs.close();
        prep.close();
    }

    @Test public void insert1000() throws SQLException {
        stat.executeUpdate("create table in1000 (a);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into in1000 values (?);");
        conn.setAutoCommit(false);
        for (int i=0; i < 1000; i++) {
            prep.setInt(1, i);
            prep.executeUpdate();
        }
        conn.commit();

        ResultSet rs = stat.executeQuery("select count(a) from in1000;");
        assertTrue(rs.next());
        assertEquals(1000, rs.getInt(1));
        rs.close();
    }

    @Ignore
    @Test public void getObject() throws SQLException {
        stat.executeUpdate("create table testobj ("
            + "c1 integer, c2 float, c3, c4 varchar, c5 bit, c6, c7);");
        PreparedStatement prep = conn.prepareStatement(
            "insert into testobj values (?,?,?,?,?,?,?);");

        prep.setInt    (1, Integer.MAX_VALUE);
        prep.setFloat  (2, Float.MAX_VALUE);
        prep.setDouble (3, Double.MAX_VALUE);
        prep.setLong   (4, Long.MAX_VALUE);
        prep.setBoolean(5, false);
        prep.setByte   (6, (byte)7);
        prep.setBytes  (7, b1);
        prep.executeUpdate();

        ResultSet rs = stat.executeQuery(
            "select c1,c2,c3,c4,c5,c6,c7 from testobj;");
        assertTrue(rs.next());

        assertEquals(Integer.MAX_VALUE, rs.getInt(1));
        assertEquals(Integer.MAX_VALUE, (int)rs.getLong(1));
        assertEquals(Float.MAX_VALUE, rs.getFloat(2));
        assertEquals(Double.MAX_VALUE, rs.getDouble(3));
        assertEquals(Long.MAX_VALUE, rs.getLong(4));
        assertFalse(rs.getBoolean(5));
        assertEquals((byte)7, rs.getByte(6));
        assertArrayEq(rs.getBytes(7), b1);

        assertNotNull(rs.getObject(1));
        assertNotNull(rs.getObject(2));
        assertNotNull(rs.getObject(3));
        assertNotNull(rs.getObject(4));
        assertNotNull(rs.getObject(5));
        assertNotNull(rs.getObject(6));
        assertNotNull(rs.getObject(7));
        assertTrue(rs.getObject(1) instanceof Integer);
        assertTrue(rs.getObject(2) instanceof Double);
        assertTrue(rs.getObject(3) instanceof Double);
        assertTrue(rs.getObject(4) instanceof String);
        assertTrue(rs.getObject(5) instanceof Integer);
        assertTrue(rs.getObject(6) instanceof Integer);
        assertTrue(rs.getObject(7) instanceof byte[]);
        rs.close();
    }

    @Test public void tokens() throws SQLException {
        /* checks for a bug where a substring is read by the driver as the
         * full original string, caused by my idiocyin assuming the
         * pascal-style string was null terminated. Thanks Oliver Randschau. */
        StringTokenizer st = new StringTokenizer("one two three");
        st.nextToken();
        String substr = st.nextToken();

        PreparedStatement prep = conn.prepareStatement("select ?;");
        prep.setString(1, substr);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(substr, rs.getString(1));
    }

    @Test public void utf() throws SQLException {
        ResultSet rs = stat.executeQuery("select '"
                +utf01+"','"+utf02+"','"+utf03+"','"+utf04+"','"
                +utf05+"','"+utf06+"','"+utf07+"','"+utf08+"';");
        assertEquals(utf01, rs.getString(1));
        assertEquals(utf02, rs.getString(2));
        assertEquals(utf03, rs.getString(3));
        assertEquals(utf04, rs.getString(4));
        assertEquals(utf05, rs.getString(5));
        assertEquals(utf06, rs.getString(6));
        assertEquals(utf07, rs.getString(7));
        assertEquals(utf08, rs.getString(8));
        rs.close();

        PreparedStatement prep = conn.prepareStatement(
            "select ?,?,?,?,?,?,?,?;");
        prep.setString(1, utf01); prep.setString(2, utf02);
        prep.setString(3, utf03); prep.setString(4, utf04);
        prep.setString(5, utf05); prep.setString(6, utf06);
        prep.setString(7, utf07); prep.setString(8, utf08);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(utf01, rs.getString(1));
        assertEquals(utf02, rs.getString(2));
        assertEquals(utf03, rs.getString(3));
        assertEquals(utf04, rs.getString(4));
        assertEquals(utf05, rs.getString(5));
        assertEquals(utf06, rs.getString(6));
        assertEquals(utf07, rs.getString(7));
        assertEquals(utf08, rs.getString(8));
        rs.close();
    }

    @Test public void batch() throws SQLException {
        ResultSet rs;

        stat.executeUpdate("create table test (c1, c2, c3, c4);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values (?,?,?,?);");
        for (int i=0; i < 10; i++) {
            prep.setInt(1, Integer.MIN_VALUE + i);
            prep.setFloat(2, Float.MIN_VALUE + i);
            prep.setString(3, "Hello " + i);
            prep.setDouble(4, Double.MAX_VALUE + i);
            prep.addBatch();
        }
        assertArrayEq(prep.executeBatch(), new int[] { 1,1,1,1,1,1,1,1,1,1 });
        assertEquals(0, prep.executeBatch().length);
        prep.close();

        rs = stat.executeQuery("select * from test;");
        for (int i=0; i < 10; i++) {
            assertTrue(rs.next());
            assertEquals(Integer.MIN_VALUE + i, rs.getInt(1));
            assertEquals(Float.MIN_VALUE + i, rs.getFloat(2));
            assertEquals("Hello " + i, rs.getString(3));
            assertEquals(Double.MAX_VALUE + i, rs.getDouble(4));
        }
        rs.close();
        stat.executeUpdate("drop table test;");
    }

    @Test public void retainKeysInBatch() throws SQLException {
        stat.executeUpdate("create table test (c1, c2);");
        PreparedStatement prep = conn.prepareStatement(
            "insert into test values (?, ?);"
        );
        prep.setInt(1, 10);  prep.setString(2, "ten");       prep.addBatch();
        prep.setInt(1, 100); prep.setString(2, "hundred");   prep.addBatch();
        prep.setString(2, "one hundred");                    prep.addBatch();
        prep.setInt(1, 1000); prep.setString(2, "thousand"); prep.addBatch();
        prep.executeBatch();
        prep.close();

        ResultSet rs = stat.executeQuery("select * from test;");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertEquals("ten", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertEquals("hundred", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertEquals("one hundred", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(1000, rs.getInt(1));
        assertEquals("thousand", rs.getString(2));
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void dblock() throws SQLException {
        stat.executeUpdate("create table test (c1);");
        stat.executeUpdate("insert into test values (1);");
        conn.prepareStatement("select * from test;").executeQuery().close();
        stat.executeUpdate("drop table test;");

    }

    @Test public void dbclose() throws SQLException {
        conn.prepareStatement("select ?;").setString(1, "Hello World");
        conn.prepareStatement("select null;").close();
        conn.prepareStatement("select null;").executeQuery().close();
        conn.prepareStatement("create table t (c);").executeUpdate();
        conn.prepareStatement("select null;");
    }

    @Test public void batchOneParam() throws SQLException {
        stat.executeUpdate("create table test (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values (?);");
        for (int i=0; i < 10; i++) {
            prep.setInt(1, Integer.MIN_VALUE + i);
            prep.addBatch();
        }
        assertArrayEq(prep.executeBatch(), new int[] { 1,1,1,1,1,1,1,1,1,1 });
        prep.close();
        ResultSet rs = stat.executeQuery("select count(*) from test;");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        rs.close();
    }

    @Test public void paramMetaData() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("select ?,?,?,?;");
        assertEquals(4, prep.getParameterMetaData().getParameterCount());
    }

    @Test public void metaData() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "select ? as col1, ? as col2, ? as delta;");
        ResultSetMetaData meta = prep.getMetaData();
        assertEquals(3, meta.getColumnCount());
        assertEquals("col1", meta.getColumnName(1));
        assertEquals("col2", meta.getColumnName(2));
        assertEquals("delta", meta.getColumnName(3));
        /*assertEquals(Types.INTEGER, meta.getColumnType(1));
        assertEquals(Types.INTEGER, meta.getColumnType(2));
        assertEquals(Types.INTEGER, meta.getColumnType(3));*/

        meta = prep.executeQuery().getMetaData();
        assertEquals(3, meta.getColumnCount());
        prep.close();
    }

    @Test public void date1() throws SQLException {
        Date d1 = new Date(987654321);

        stat.execute("create table t (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into t values(?);");
        prep.setDate(1, d1);
        prep.executeUpdate();
        prep.setDate(1, null);
        prep.executeUpdate();

        ResultSet rs = stat.executeQuery("select c1 from t;");
        assertTrue(rs.next());
        assertEquals(d1.getTime(), rs.getLong(1));
        assertTrue(rs.getDate(1).equals(d1));
        assertTrue(rs.next());
        assertEquals(null, rs.getDate(1));
        rs.close();
    }

    @Test public void date2() throws SQLException {
        Date d1 = new Date(1092941466000L);
        stat.execute("create table t (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into t values (datetime(?/1000, 'unixepoch'));");
        prep.setDate(1, d1);
        prep.executeUpdate();

        ResultSet rs = stat.executeQuery(
            "select strftime('%s', c1) * 1000 from t;");
        assertTrue(rs.next());
        assertEquals(d1.getTime(), rs.getLong(1));
        assertTrue(rs.getDate(1).equals(d1));
    }

    @SuppressWarnings("deprecation")
    @Test public void date() throws SQLException {
        connectWithJulianDayModeActivated();
        Date d1 = new Date(System.currentTimeMillis());

        stat.execute("create table t (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into t values(?);");
        prep.setDate(1, d1);
        prep.executeUpdate();

        ResultSet rs = stat.executeQuery("select c1 from t;");
        assertTrue(rs.next());
        assertEquals(d1.getYear(), rs.getDate(1).getYear());
        assertEquals(d1.getMonth(), rs.getDate(1).getMonth());
        assertEquals(d1.getDay(), rs.getDate(1).getDay());
        rs.close();
    }

    @SuppressWarnings("deprecation")
    @Test public void time() throws SQLException {
        connectWithJulianDayModeActivated();
        Time d1 = new Time(System.currentTimeMillis());

        stat.execute("create table t (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into t values (?);");
        prep.setTime(1, d1);
        prep.executeUpdate();

        ResultSet rs = stat.executeQuery("select c1 from t;");
        assertTrue(rs.next());
        assertEquals(d1.getHours(), rs.getTime(1).getHours());
        assertEquals(d1.getMinutes(), rs.getTime(1).getMinutes());
        assertEquals(d1.getSeconds(), rs.getTime(1).getSeconds());
    }

    @Test public void timestamp() throws SQLException {
        connectWithJulianDayModeActivated();
        long now = System.currentTimeMillis();
        Timestamp d1 = new Timestamp(now);
        Date d2 = new Date(now);
        Time d3 = new Time(now);

        stat.execute("create table t (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into t values (?);");
        prep.setTimestamp(1, d1);
        prep.executeUpdate();

        ResultSet rs = stat.executeQuery("select c1 from t;");
        assertTrue(rs.next());
        assertEquals(d1, rs.getTimestamp(1));

        rs = stat.executeQuery("select date(c1, 'localtime') from t;");
        assertTrue(rs.next());
        assertEquals(d2.toString(), rs.getString(1));

        rs = stat.executeQuery("select time(c1, 'localtime') from t;");
        assertTrue(rs.next());
        assertEquals(d3.toString(), rs.getString(1));

        rs = stat.executeQuery("select strftime('%Y-%m-%d %H:%M:%f', c1, 'localtime') from t;");
        assertTrue(rs.next());
        // assertEquals(d1.toString(), rs.getString(1)); // ms are not occurate...
    }

    @Test public void changeSchema() throws SQLException {
        stat.execute("create table t (c1);");
        PreparedStatement prep = conn.prepareStatement(
                "insert into t values (?);");
        conn.createStatement().execute("create table t2 (c2);");
        prep.setInt(1, 1000);
        prep.execute();
        prep.executeUpdate();
    }

    @Test public void reusingSetValues() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("select ?,?;");
        prep.setInt(1, 9);

        for (int i=0; i < 10; i++) {
            prep.setInt(2, i);
            ResultSet rs = prep.executeQuery();
            assertTrue(rs.next());
            assertEquals(9, rs.getInt(1));
            assertEquals(i, rs.getInt(2));
        }

        for (int i=0; i < 10; i++) {
            prep.setInt(2, i);
            ResultSet rs = prep.executeQuery();
            assertTrue(rs.next());
            assertEquals(9, rs.getInt(1));
            assertEquals(i, rs.getInt(2));
            rs.close();
        }

        prep.close();
    }

    @Test public void setmaxrows() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
            "select 1 union select 2;");
        prep.setMaxRows(1);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        prep.close();
    }

    @Test public void doubleclose() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("select null;");
        ResultSet rs = prep.executeQuery();
        rs.close();
        prep.close();
        prep.close();
    }

    @Test(expected= SQLException.class)
    public void noSuchTable() throws SQLException {
        PreparedStatement prep =
            conn.prepareStatement("select * from doesnotexist;");
        prep.executeQuery();
    }
    
    @Test(expected= SQLException.class)
    public void noSuchCol() throws SQLException {
        PreparedStatement prep =
            conn.prepareStatement("select notacol from (select 1);");
        prep.executeQuery();
    }

    @Test(expected= SQLException.class)
    public void noSuchColName() throws SQLException {
        ResultSet rs = conn.prepareStatement("select 1;").executeQuery();
        assertTrue(rs.next());
        rs.getInt("noSuchColName");
    }

    private void assertArrayEq(byte[] a, byte[] b) {
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(a.length, b.length);
        for (int i=0; i < a.length; i++)
            assertEquals(a[i], b[i]);
    }
    private void assertArrayEq(int[] a, int[] b) {
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(a.length, b.length);
        for (int i=0; i < a.length; i++)
            assertEquals(a[i], b[i]);
    }
}
