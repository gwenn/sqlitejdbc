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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a JDBC ResultSet.
 */
final class RS extends UnusedRS implements ResultSet, ResultSetMetaData, Codes
{
    private final Stmt stmt;
    private final DB db;

    boolean open = false   ;  // true means have results and can iterate them
    int maxRows;              // max. number of rows as set by a Statement
    String[] cols = null;     // if null, the RS is closed()
    Map<String, Integer> columnNameToIndex = null;
    String[] colsMeta = null; // same as cols, but used by Meta interface
    boolean[][] meta = null;

    private int row = 0;   // number of current row, starts at 1
    private int lastCol;   // last column accessed, for wasNull(). -1 if none

    RS(Stmt stmt) {
        this.stmt = stmt;
        this.db   = stmt.db;
    }


    // INTERNAL FUNCTIONS ///////////////////////////////////////////

    boolean isOpen() { return open; }

    /* Throws SQLException if ResultSet is not open. */
    void checkOpen() throws SQLException {
        if (!open) throw new SQLException("ResultSet closed");
    }

    // takes col in [1,x] form, returns in [0,x-1] form
    private int checkCol(int col) throws SQLException {
        if (colsMeta == null) throw new IllegalStateException(
            "SQLite JDBC: inconsistent internal state");
        if (col < 1 || col > colsMeta.length) throw new SQLException(
            "column " + col + " out of bounds [1," + colsMeta.length + "]");
        return --col;
    }

    // takes col in [1,x] form, marks it as last accessed and returns [0,x-1]
    private int markCol(int col) throws SQLException {
        checkOpen(); checkCol(col); lastCol = col; return --col;
    }

    private void checkMeta() throws SQLException {
        checkCol(1);
        if (meta == null) meta = db.column_metadata(stmt.pointer);
    }


    // ResultSet Functions //////////////////////////////////////////

    public boolean isClosed() throws SQLException {
        return !open;
    }

    public void close() throws SQLException {
        cols = null;
        columnNameToIndex = null;
        colsMeta = null;
        meta = null;
        open = false;
        row = 0;
        lastCol = -1;

        if (stmt == null)
            return;
        if (stmt.pointer != 0)
            db.reset(stmt.pointer);
    }

    // returns col in [1,x] form
    public int findColumn(String col) throws SQLException {
        checkOpen();
        Integer index = findColumnIndexInCache(col);
        if (null != index) {
          return index;
        }
        for (int i=0; i < cols.length; i++) {
            if (col.equalsIgnoreCase(cols[i])) {
              addColumnIndexInCache(col, i+1);
              return i+1;
            }
        }
        throw new SQLException("no such column: '"+col+"'");
    }

    private Integer findColumnIndexInCache(String col) {
        if (null == columnNameToIndex) {
            return null;
        } else {
            return columnNameToIndex.get(col);
        }
    }
    private void addColumnIndexInCache(String col, int index) {
        if (null == columnNameToIndex) {
            columnNameToIndex = new HashMap<String, Integer>(cols.length);
        }
        columnNameToIndex.put(col, index);
    }

    public boolean next() throws SQLException {
        if (!open) return false;  // finished ResultSet
        lastCol = -1;

        // first row is loaded by execute(), so do not step() again
        if (row == 0) { row++; return true; } else { row++; }

        // check if we are row limited by the statement or the ResultSet
        if (maxRows != 0 && row > maxRows) return false;

        // do the real work
        switch (db.step(stmt.pointer)) {
            case SQLITE_DONE:
                close();      // agressive closing to avoid writer starvation
                return false;
            case SQLITE_ROW: row++; return true;
            case SQLITE_BUSY:
                throw new SQLException("database locked");
            default:
                 db.throwex(); return false;
        }
    }

    public int getType() throws SQLException { return TYPE_FORWARD_ONLY; }

    public int getFetchSize() throws SQLException { return 1; }
    public void setFetchSize(int rows) throws SQLException {
        if (rows != 1) {
            throw new SQLException("SQLite does not support setting fetch size");
        }
    }

    public int getFetchDirection() throws SQLException {
        checkOpen(); return ResultSet.FETCH_FORWARD; }
    public void setFetchDirection(int d) throws SQLException {
        checkOpen();
        if (d != ResultSet.FETCH_FORWARD)
            throw new SQLException("only FETCH_FORWARD direction supported");
    }

    public boolean isAfterLast() throws SQLException { return !open; }
    public boolean isBeforeFirst() throws SQLException {
        return open && row == 0; }
    public boolean isFirst() throws SQLException { return row == 1; }
    public boolean isLast() throws SQLException { // FIXME
        throw new SQLException("function not yet implemented for SQLite"); }

    protected void finalize() throws SQLException { close(); }

    public int getRow() throws SQLException { return row; }

    public boolean wasNull() throws SQLException {
        return db.column_type(stmt.pointer, markCol(lastCol)) == SQLITE_NULL;
    }


    // DATA ACCESS FUNCTIONS ////////////////////////////////////////

    public boolean getBoolean(int col) throws SQLException {
        return getInt(col) != 0; }
    public boolean getBoolean(String col) throws SQLException {
        return getBoolean(findColumn(col)); }

    public byte getByte(int col) throws SQLException {
        return (byte)getInt(col); }
    public byte getByte(String col) throws SQLException {
        return getByte(findColumn(col)); }

    public byte[] getBytes(int col) throws SQLException {
        return db.column_blob(stmt.pointer, markCol(col)); }
    public byte[] getBytes(String col) throws SQLException {
        return getBytes(findColumn(col)); }

    public Date getDate(int col) throws SQLException {
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return null;
        if (db.isJulianDayMode()) {
            final double jd = db.column_double(stmt.pointer, markCol(col));
            return new Date(fromJulianDay(jd));
        } else {
            return new Date(db.column_long(stmt.pointer, markCol(col)));
        }
    }
    public Date getDate(int col, Calendar cal) throws SQLException {
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return null;
        if (cal == null) return getDate(col);
        if (db.isJulianDayMode()) {
            final double jd = db.column_double(stmt.pointer, markCol(col));
            cal.setTimeInMillis(fromJulianDay(jd));
            return new Date(cal.getTime().getTime());
        } else {
            cal.setTimeInMillis(db.column_long(stmt.pointer, markCol(col)));
            return new Date(cal.getTime().getTime());
        }
    }
    public Date getDate(String col) throws SQLException {
        return getDate(findColumn(col)); }
    public Date getDate(String col, Calendar cal) throws SQLException {
        return getDate(findColumn(col), cal); }

    public double getDouble(int col) throws SQLException {
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return 0;
        return db.column_double(stmt.pointer, markCol(col));
    }
    public double getDouble(String col) throws SQLException {
        return getDouble(findColumn(col)); }

    public float getFloat(int col) throws SQLException {
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return 0;
        return (float)db.column_double(stmt.pointer, markCol(col));
    }
    public float getFloat(String col) throws SQLException {
        return getFloat(findColumn(col)); }

    public int getInt(int col) throws SQLException {
        return db.column_int(stmt.pointer, markCol(col)); }
    public int getInt(String col) throws SQLException {
        return getInt(findColumn(col)); }

    public long getLong(int col) throws SQLException {
        return db.column_long(stmt.pointer, markCol(col)); }
    public long getLong(String col) throws SQLException {
        return getLong(findColumn(col)); }

    public short getShort(int col) throws SQLException {
        return (short)getInt(col); }
    public short getShort(String col) throws SQLException {
        return getShort(findColumn(col)); }

    public String getString(int col) throws SQLException {
        return db.column_text(stmt.pointer, markCol(col)); }
    public String getString(String col) throws SQLException {
        return getString(findColumn(col)); }

    public Time getTime(int col) throws SQLException {
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return null;
        if (db.isJulianDayMode()) {
            final double jd = db.column_double(stmt.pointer, markCol(col));
            return new Time(fromJulianDay(jd));
        } else {
            return new Time(db.column_long(stmt.pointer, markCol(col)));
        }
    }
    public Time getTime(int col, Calendar cal) throws SQLException {
        if (cal == null) return getTime(col);
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return null;
        if (db.isJulianDayMode()) {
            final double jd = db.column_double(stmt.pointer, markCol(col));
            cal.setTimeInMillis(fromJulianDay(jd));
            return new Time(cal.getTime().getTime());
        } else {
            cal.setTimeInMillis(db.column_long(stmt.pointer, markCol(col)));
            return new Time(cal.getTime().getTime());
        }
    }
    public Time getTime(String col) throws SQLException {
        return getTime(findColumn(col)); }
    public Time getTime(String col, Calendar cal) throws SQLException {
        return getTime(findColumn(col), cal); }

    public Timestamp getTimestamp(int col) throws SQLException {
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return null;
        if (db.isJulianDayMode()) {
            final double jd = db.column_double(stmt.pointer, markCol(col));
            return new Timestamp(fromJulianDay(jd));
        } else {
            return new Timestamp(db.column_long(stmt.pointer, markCol(col)));
        }
    }
    public Timestamp getTimestamp(int col, Calendar cal) throws SQLException {
        if (cal == null) return getTimestamp(col);
        if (db.column_type(stmt.pointer, markCol(col)) == SQLITE_NULL)
            return null;
        if (db.isJulianDayMode()) {
            final double jd = db.column_double(stmt.pointer, markCol(col));
            cal.setTimeInMillis(fromJulianDay(jd));
            return new Timestamp(cal.getTime().getTime());
        } else {
            cal.setTimeInMillis(db.column_long(stmt.pointer, markCol(col)));
            return new Timestamp(cal.getTime().getTime());
        }
    }
    public Timestamp getTimestamp(String col) throws SQLException {
        return getTimestamp(findColumn(col)); }
    public Timestamp getTimestamp(String c, Calendar ca) throws SQLException {
        return getTimestamp(findColumn(c), ca); }

    public String getNString(int col) throws SQLException {
        return getString(col);
    }

    public String getNString(String col) throws SQLException {
        return getNString(findColumn(col));
    }

    public Reader getNCharacterStream(int col) throws SQLException {
        return getCharacterStream(col);
    }

    public Reader getNCharacterStream(String col) throws SQLException {
        return getNCharacterStream(findColumn(col));
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;  // FIXME
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;  // FIXME
    }

    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    public BigDecimal getBigDecimal(int col) throws SQLException {
        final String stringValue = getString(col);
        if (stringValue == null) {
            return null;
        } else {
            try {
                return new BigDecimal(stringValue);
            } catch (NumberFormatException e) {
                throw new SQLException("Bad value for type BigDecimal : " + stringValue);
            }
        }
    }
    public BigDecimal getBigDecimal(String col) throws SQLException {
        return getBigDecimal(findColumn(col));
    }

    public InputStream getBinaryStream(int col) throws SQLException {
        final byte[] bytes = getBytes(col);
        if (bytes == null) {
            return null;
        } else {
            return new ByteArrayInputStream(bytes);
        }
    }
    public InputStream getBinaryStream(String col) throws SQLException {
        return getBinaryStream(findColumn(col));
    }

    public Reader getCharacterStream(int col) throws SQLException {
        final String string = getString(col);
        if (string == null) {
            return null;
        } else {
            return new CharArrayReader(string.toCharArray());
        }
    }
    public Reader getCharacterStream(String col) throws SQLException {
        return getCharacterStream(findColumn(col));
    }

    public InputStream getAsciiStream(int col) throws SQLException {
        final String string = getString(col);
        if (string == null) {
            return null;
        } else {
            try {
                return new ByteArrayInputStream(string.getBytes("ASCII"));
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e.getMessage());
            }
        }
    }
    public InputStream getAsciiStream(String col) throws SQLException {
        return getAsciiStream(findColumn(col));
    }

    public Object getObject(int col) throws SQLException {
        switch (db.column_type(stmt.pointer, checkCol(col))) {
            case SQLITE_INTEGER:
                long val = getLong(col);
                if (val > (long)Integer.MAX_VALUE
                        || val < (long)Integer.MIN_VALUE)
                    return val;
                else
                    return (int) val;
            case SQLITE_FLOAT:   return getDouble(col);
            case SQLITE_BLOB:    return getBytes(col);
            case SQLITE_NULL:    return null;
            case SQLITE_TEXT:
            default:
                return getString(col);
        }
    }
    public Object getObject(String col) throws SQLException {
        return getObject(findColumn(col)); }

    public Statement getStatement() { return stmt; }
    public String getCursorName() throws SQLException { return null; }
    public SQLWarning getWarnings() throws SQLException { return null; }
    public void clearWarnings() throws SQLException {}

    // ResultSetMetaData Functions //////////////////////////////////

    // we do not need to check the RS is open, only that colsMeta
    // is not null, done with checkCol(int).

    public ResultSetMetaData getMetaData() throws SQLException {
        return this; }

    public String getCatalogName(int col) throws SQLException { // FIXME
        return db.column_table_name(stmt.pointer, checkCol(col)); }
    public String getColumnClassName(int col) throws SQLException { // FIXME
        checkCol(col); return "java.lang.Object"; }
    public int getColumnCount() throws SQLException {
        checkCol(1); return colsMeta.length;
    }
    public int getColumnDisplaySize(int col) throws SQLException { // FIXME
        return Integer.MAX_VALUE; }
    public String getColumnLabel(int col) throws SQLException {
        return getColumnName(col); }
    public String getColumnName(int col) throws SQLException {
        return db.column_name(stmt.pointer, checkCol(col)); }
    public int getColumnType(int col) throws SQLException {
        switch (db.column_type(stmt.pointer, checkCol(col))) {
            case SQLITE_INTEGER: return Types.INTEGER;
            case SQLITE_FLOAT:   return Types.FLOAT;
            case SQLITE_BLOB:    return Types.BLOB;
            case SQLITE_NULL:    return Types.NULL;
            case SQLITE_TEXT:
            default:
                return Types.VARCHAR;
        }
    }
    public String getColumnTypeName(int col) throws SQLException {
        switch (db.column_type(stmt.pointer, checkCol(col))) {
            case SQLITE_INTEGER: return "integer";
            case SQLITE_FLOAT:   return "float";
            case SQLITE_BLOB:    return "blob";
            case SQLITE_NULL:    return "null";
            case SQLITE_TEXT:
            default:             return "text";
        }
    }
    public int getPrecision(int col) throws SQLException { return 0; } // FIXME
    public int getScale(int col) throws SQLException { return 0; }
    public String getSchemaName(int col) throws SQLException { return ""; }
    public String getTableName(int col) throws SQLException {
        return db.column_table_name(stmt.pointer, checkCol(col)); }
    public int isNullable(int col) throws SQLException {
        checkMeta();
        return meta[checkCol(col)][1] ? columnNoNulls: columnNullable;
    }
    public boolean isAutoIncrement(int col) throws SQLException { // TODO Validate
        checkMeta(); return meta[checkCol(col)][2]; }
    public boolean isCaseSensitive(int col) throws SQLException { return true; }
    public boolean isCurrency(int col) throws SQLException { return false; }
    public boolean isDefinitelyWritable(int col) throws SQLException {
        return true; } // FIXME: check db file constraints?
    public boolean isReadOnly(int col) throws SQLException { return false; }
    public boolean isSearchable(int col) throws SQLException { return true; }
    public boolean isSigned(int col) throws SQLException { return false; }
    public boolean isWritable(int col) throws SQLException { return true; }

    public int getConcurrency() throws SQLException { return CONCUR_READ_ONLY; }

    public boolean rowDeleted()  throws SQLException { return false; } // FIXME
    public boolean rowInserted() throws SQLException { return false; }
    public boolean rowUpdated()  throws SQLException { return false; }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not a wrapper");
    }
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // 1970-01-01 00:00:00 is JD 2440587.5
    private static long fromJulianDay(double jd) {
        jd -= 2440587.5;
        jd *= 86400000.0;
        return (long) jd;
    }
}
