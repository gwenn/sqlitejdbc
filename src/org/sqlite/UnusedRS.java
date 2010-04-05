/*
 * The author disclaims copyright to this source code.  In place of
 * a legal notice, here is a blessing:
 *
 *    May you do good and not evil.
 *    May you find forgiveness for yourself and forgive others.
 *    May you share freely, never taking more than you give.
 *
 */
package org.sqlite;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/** Unused JDBC functions from ResultSet.  */
abstract class UnusedRS implements ResultSet {
    private static SQLException typeForwardOnly() {
        return new SQLException("ResultSet is TYPE_FORWARD_ONLY");
    }
    private static SQLException concurReadOnly() {
        return new SQLException("ResultSet is CONCUR_READ_ONLY");
    }
    
    public Array getArray(int i)
        throws SQLException { throw Util.unsupported(); }
    public Array getArray(String col)
        throws SQLException { throw Util.unsupported(); }
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int col, int s)
        throws SQLException { throw Util.unsupported(); }
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(String col, int s)
        throws SQLException { throw Util.unsupported(); }
    public Blob getBlob(int col)
        throws SQLException { throw Util.unsupported(); }
    public Blob getBlob(String col)
        throws SQLException { throw Util.unsupported(); }
    public Clob getClob(int col)
        throws SQLException { throw Util.unsupported(); }
    public Clob getClob(String col)
        throws SQLException { throw Util.unsupported(); }
    public Object getObject(int col, Map<String, Class<?>> map)
        throws SQLException { throw Util.unsupported(); }
    public Object getObject(String col, Map<String, Class<?>> map)
        throws SQLException { throw Util.unsupported(); }
    public Ref getRef(int i)
        throws SQLException { throw Util.unsupported(); }
    public Ref getRef(String col)
        throws SQLException { throw Util.unsupported(); }

    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int col)
        throws SQLException { throw Util.unsupported(); }
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String col)
        throws SQLException { throw Util.unsupported(); }
    public URL getURL(int col)
        throws SQLException { throw Util.unsupported(); }
    public URL getURL(String col)
        throws SQLException { throw Util.unsupported(); }

    public NClob getNClob(int col)
        throws SQLException { throw Util.unsupported(); }
    public NClob getNClob(String col)
        throws SQLException { throw Util.unsupported(); }
    public SQLXML getSQLXML(int col)
        throws SQLException { throw Util.unsupported(); }
    public SQLXML getSQLXML(String col)
        throws SQLException { throw Util.unsupported(); }
    public RowId getRowId(int col)
        throws SQLException { throw Util.unsupported(); }
    public RowId getRowId(String col)
        throws SQLException { throw Util.unsupported(); }

    public void cancelRowUpdates()
        throws SQLException { throw concurReadOnly(); }
    public void deleteRow()
        throws SQLException { throw concurReadOnly(); }
    public void insertRow() throws SQLException {
        throw concurReadOnly(); }
    public void moveToCurrentRow() throws SQLException {
        throw concurReadOnly(); }
    public void moveToInsertRow() throws SQLException {
        throw concurReadOnly(); }
    public void refreshRow()
        throws SQLException { throw concurReadOnly(); }
    public void updateRow()
        throws SQLException { throw concurReadOnly(); }

    public boolean last() throws SQLException {
        throw typeForwardOnly(); }
    public boolean previous() throws SQLException {
        throw typeForwardOnly(); }
    public boolean relative(int rows) throws SQLException {
        throw typeForwardOnly(); }
    public boolean absolute(int row) throws SQLException {
        throw typeForwardOnly(); }
    public void afterLast() throws SQLException {
        throw typeForwardOnly(); }
    public void beforeFirst() throws SQLException {
        throw typeForwardOnly(); }
    public boolean first() throws SQLException {
        throw typeForwardOnly(); }

    public void updateArray(int col, Array x)
        throws SQLException { throw concurReadOnly(); }
    public void updateArray(String col, Array x)
        throws SQLException { throw concurReadOnly(); }
    public void updateAsciiStream(int col, InputStream x, int l)
        throws SQLException { throw concurReadOnly(); }
    public void updateAsciiStream(String col, InputStream x, int l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBigDecimal(int col, BigDecimal x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBigDecimal(String col, BigDecimal x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBinaryStream(int c, InputStream x, int l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBinaryStream(String c, InputStream x, int l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBlob(int col, Blob x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBlob(String col, Blob x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBoolean(int col, boolean x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBoolean(String col, boolean x)
        throws SQLException { throw concurReadOnly(); }
    public void updateByte(int col, byte x)
        throws SQLException { throw concurReadOnly(); }
    public void updateByte(String col, byte x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBytes(int col, byte[] x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBytes(String col, byte[] x)
        throws SQLException { throw concurReadOnly(); }
    public void updateCharacterStream(int c, Reader x, int l)
        throws SQLException { throw concurReadOnly(); }
    public void updateCharacterStream(String c, Reader r, int l)
        throws SQLException { throw concurReadOnly(); }
    public void updateClob(int col, Clob x)
        throws SQLException { throw concurReadOnly(); }
    public void updateClob(String col, Clob x)
        throws SQLException { throw concurReadOnly(); }
    public void updateDate(int col, Date x)
        throws SQLException { throw concurReadOnly(); }
    public void updateDate(String col, Date x)
        throws SQLException { throw concurReadOnly(); }
    public void updateDouble(int col, double x)
        throws SQLException { throw concurReadOnly(); }
    public void updateDouble(String col, double x)
        throws SQLException { throw concurReadOnly(); }
    public void updateFloat(int col, float x)
        throws SQLException { throw concurReadOnly(); }
    public void updateFloat(String col, float x)
        throws SQLException { throw concurReadOnly(); }
    public void updateInt(int col, int x)
        throws SQLException { throw concurReadOnly(); }
    public void updateInt(String col, int x)
        throws SQLException { throw concurReadOnly(); }
    public void updateLong(int col, long x)
        throws SQLException { throw concurReadOnly(); }
    public void updateLong(String col, long x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNull(int col)
        throws SQLException { throw concurReadOnly(); }
    public void updateNull(String col)
        throws SQLException { throw concurReadOnly(); }
    public void updateObject(int c, Object x)
        throws SQLException { throw concurReadOnly(); }
    public void updateObject(int c, Object x, int s)
        throws SQLException { throw concurReadOnly(); }
    public void updateObject(String col, Object x)
        throws SQLException { throw concurReadOnly(); }
    public void updateObject(String c, Object x, int s)
        throws SQLException { throw concurReadOnly(); }
    public void updateRef(int col, Ref x)
        throws SQLException { throw concurReadOnly(); }
    public void updateRef(String c, Ref x)
        throws SQLException { throw concurReadOnly(); }
    public void updateShort(int c, short x)
        throws SQLException { throw concurReadOnly(); }
    public void updateShort(String c, short x)
        throws SQLException { throw concurReadOnly(); }
    public void updateString(int c, String x)
        throws SQLException { throw concurReadOnly(); }
    public void updateString(String c, String x)
        throws SQLException { throw concurReadOnly(); }
    public void updateTime(int c, Time x)
        throws SQLException { throw concurReadOnly(); }
    public void updateTime(String c, Time x)
        throws SQLException { throw concurReadOnly(); }
    public void updateTimestamp(int c, Timestamp x)
        throws SQLException { throw concurReadOnly(); }
    public void updateTimestamp(String c, Timestamp x)
        throws SQLException { throw concurReadOnly(); }

    public void updateNString(int c, String x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNString(String c, String x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNClob(int c, NClob x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNClob(String c, NClob x)
        throws SQLException { throw concurReadOnly(); }

    public void updateSQLXML(int c, SQLXML x)
        throws SQLException { throw concurReadOnly(); }
    public void updateSQLXML(String c, SQLXML x)
        throws SQLException { throw concurReadOnly(); }

    public void updateNCharacterStream(int c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateNCharacterStream(String c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateAsciiStream(int c, InputStream x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBinaryStream(int c, InputStream x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateCharacterStream(int c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateAsciiStream(String c, InputStream x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBinaryStream(String c, InputStream x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateCharacterStream(String c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBlob(int c, InputStream x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateBlob(String c, InputStream x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateClob(int c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateClob(String c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateNClob(int c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateNClob(String c, Reader x, long l)
        throws SQLException { throw concurReadOnly(); }
    public void updateNCharacterStream(int c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNCharacterStream(String c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateAsciiStream(int c, InputStream x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBinaryStream(int c, InputStream x)
        throws SQLException { throw concurReadOnly(); }
    public void updateCharacterStream(int c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateAsciiStream(String c, InputStream x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBinaryStream(String c, InputStream x)
        throws SQLException { throw concurReadOnly(); }
    public void updateCharacterStream(String c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBlob(int c, InputStream x)
        throws SQLException { throw concurReadOnly(); }
    public void updateBlob(String c, InputStream x)
        throws SQLException { throw concurReadOnly(); }
    public void updateClob(int c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateClob(String c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNClob(int c, Reader x)
        throws SQLException { throw concurReadOnly(); }
    public void updateNClob(String c, Reader x)
        throws SQLException { throw concurReadOnly(); }

    public void updateRowId(int c, RowId x)
        throws SQLException { throw concurReadOnly(); }
    public void updateRowId(String c, RowId x)
        throws SQLException { throw concurReadOnly(); }
}
