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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

final class PrepStmt extends Stmt
        implements PreparedStatement, ParameterMetaData, Codes
{
    private int columnCount;
    private int paramCount;

    PrepStmt(Conn conn, String sql) throws SQLException {
        super(conn);

        this.sql = sql;
        db.prepare(this);
        rs.colsMeta = db.column_names(pointer);
        columnCount = db.column_count(pointer);
        paramCount = db.bind_parameter_count(pointer);
        batch = new Object[paramCount];
        batchPos = 0;
    }

    public void clearParameters() throws SQLException {
        checkOpen();
        db.reset(pointer);
        clearBatch();
    }

    protected void finalize() throws SQLException { close(); }

    public boolean execute() throws SQLException {
        checkOpen();
        rs.close();
        db.reset(pointer);
        resultsWaiting = db.execute(this, batch);
        return columnCount != 0;
    }

    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        if (columnCount == 0)
            throw new SQLException("query does not return results");
        rs.close();
        db.reset(pointer);
        resultsWaiting = db.execute(this, batch);
        return getResultSet();
    }

    public int executeUpdate() throws SQLException {
        checkOpen();
        if (columnCount != 0)
            throw new SQLException("query returns results");
        rs.close();
        db.reset(pointer);
        return db.executeUpdate(this, batch);
    }

    public int[] executeBatch() throws SQLException {
        if (batchPos == 0) return new int[] {};
        try {
            return db.executeBatch(pointer, batchPos / paramCount, batch);
        } finally {
            clearBatch();
        }
    }

    public int getUpdateCount() throws SQLException {
        checkOpen();
        if (pointer == 0 || resultsWaiting) return -1;
        return db.changes();
    }

    public void addBatch() throws SQLException {
        checkOpen();
        batchPos += paramCount;
        if (batchPos + paramCount > batch.length) {
            Object[] nb = new Object[batch.length * 2];
            System.arraycopy(batch, 0, nb, 0, batch.length);
            batch = nb;
        }
        System.arraycopy(batch, batchPos - paramCount,
                         batch, batchPos, paramCount);
    }


    // ParameterMetaData FUNCTIONS //////////////////////////////////

    public ParameterMetaData getParameterMetaData() { return this; }

    public int getParameterCount() throws SQLException {
        checkOpen(); return paramCount; }
    public String getParameterClassName(int param) throws SQLException {
        checkOpen(); return "java.lang.String"; }
    public String getParameterTypeName(int pos) { return "VARCHAR"; }
    public int getParameterType(int pos) { return Types.VARCHAR; }
    public int getParameterMode(int pos) { return parameterModeIn; }
    public int getPrecision(int pos) { return 0; }
    public int getScale(int pos) { return 0; }
    public int isNullable(int pos) { return parameterNullable; }
    public boolean isSigned(int pos) { return true; }
    public Statement getStatement() { return this; }


    // PARAMETER FUNCTIONS //////////////////////////////////////////

    private void batch(int pos, Object value) throws SQLException {
        checkOpen();
        if (batch == null) batch = new Object[paramCount];
        batch[batchPos + pos - 1] = value;
    }

    public void setBoolean(int pos, boolean value) throws SQLException {
        setInt(pos, value ? 1 : 0);
    }
    public void setByte(int pos, byte value) throws SQLException {
        setInt(pos, (int)value);
    }
    public void setBytes(int pos, byte[] value) throws SQLException {
        batch(pos, value);
    }
    public void setDouble(int pos, double value) throws SQLException {
        batch(pos, value);
    }
    public void setFloat(int pos, float value) throws SQLException {
        setDouble(pos, value);
    }
    public void setInt(int pos, int value) throws SQLException {
        batch(pos, value);
    }
    public void setLong(int pos, long value) throws SQLException {
        batch(pos, value);
    }
    public void setNull(int pos, int u1) throws SQLException {
        setNull(pos, u1, null);
    }
    public void setNull(int pos, int u1, String u2) throws SQLException {
        batch(pos, null);
    }
    public void setObject(int pos, Object value) throws SQLException {
        if (value == null) {
          setNull(pos, Types.NULL);
        } else if (value instanceof String) {
          setString(pos, (String)value);
        } else if (value instanceof BigDecimal) {
          setBigDecimal(pos, (BigDecimal)value);
        } else if (value instanceof Short) {
          setShort(pos, ((Short)value).shortValue());
        } else if (value instanceof Integer) {
          setInt(pos, ((Integer)value).intValue());
        } else if (value instanceof Long) {
          setLong(pos, ((Long)value).longValue());
        } else if (value instanceof Float) {
          setFloat(pos, ((Float)value).floatValue());
        } else if (value instanceof Double) {
          setDouble(pos, ((Double)value).doubleValue());
        } else if (value instanceof byte[]) {
          setBytes(pos, (byte[])value);
        } else if (value instanceof Date) {
          setDate(pos, (Date)value);
        } else if (value instanceof Time) {
          setTime(pos, (Time)value);
        } else if (value instanceof Timestamp) {
          setTimestamp(pos, (Timestamp)value);
        } else if (value instanceof Boolean) {
          setBoolean(pos, ((Boolean)value).booleanValue());
        } else if (value instanceof Byte) {
          setByte(pos, ((Byte)value).byteValue());
        } else if (value instanceof Blob) {
          setBlob(pos, (Blob)value);
        } else if (value instanceof Clob) {
          setClob(pos, (Clob)value);
        } else if (value instanceof Array) {
          setArray(pos, (Array)value);
        } else {
          throw new SQLException("Can't infer type for " + value.getClass().getName() + '.');
        }
    }
    public void setObject(int p, Object v, int t) throws SQLException {
        setObject(p, v); }
    public void setObject(int p, Object v, int t, int s) throws SQLException {
        setObject(p, v); }
    public void setShort(int pos, short value) throws SQLException {
        setInt(pos, (int)value); }
    public void setString(int pos, String value) throws SQLException {
        batch(pos, value);
    }
    public void setDate(int pos, Date x) throws SQLException {
        if (db.isJulianDayMode())
            batch(pos, x == null ? null : toJulianDay(x.getTime()));
        else
            batch(pos, x == null ? null : x.getTime());
    }
    public void setDate(int pos, Date x, Calendar cal) throws SQLException {
        setDate(pos, x); }
    public void setTime(int pos, Time x) throws SQLException {
        if (db.isJulianDayMode())
            batch(pos, x == null ? null : toJulianDay(x.getTime()));
        else
            batch(pos, x == null ? null : x.getTime());
    }
    public void setTime(int pos, Time x, Calendar cal) throws SQLException {
        setTime(pos, x); }
    public void setTimestamp(int pos, Timestamp x) throws SQLException {
        if (db.isJulianDayMode())
            batch(pos, x == null ? null : toJulianDay(x.getTime()));
        else
            batch(pos, x == null ? null : x.getTime());
    }
    public void setTimestamp(int pos, Timestamp x, Calendar cal)
            throws SQLException {
        setTimestamp(pos, x);
    }

    public void setBigDecimal(int pos, BigDecimal value) throws SQLException {
        batch(pos, value == null ? null : value.toString());
    }

    public void setBinaryStream(int pos, InputStream x, int length) throws SQLException {
        if (x == null) {
            batch(pos, null);
        } else {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try {
                copy(x, output);
            } catch (IOException e) {
                throw new SQLException(e.getMessage());
            }
            batch(pos, output.toByteArray());
        }
    }
    public void setCharacterStream(int pos, Reader reader, int length) throws SQLException {
        if (reader == null) {
            batch(pos, null);
        } else {
            final StringWriter sw = new StringWriter();
            try {
                copy(reader, sw);
            } catch (IOException e) {
                throw new SQLException(e.getMessage());
            }
            batch(pos, sw.toString());
        }
    }
    public void setAsciiStream(int pos, InputStream x, int length) throws SQLException {
        try {
            setCharacterStream(pos, x == null ? null : new InputStreamReader(x, "ASCII"), length);
        } catch (UnsupportedEncodingException e) {
            throw new SQLException(e.getMessage());
        }
    }

    private static void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[1024 * 4];
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }
    private static void copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[1024 * 4];
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen(); return rs; }


    // UNUSED ///////////////////////////////////////////////////////

    public boolean execute(String sql)
        throws SQLException { throw unused(); }
    public int executeUpdate(String sql)
        throws SQLException { throw unused(); }
    public ResultSet executeQuery(String sql)
        throws SQLException { throw unused(); }
    public void addBatch(String sql)
        throws SQLException { throw unused(); }

    private SQLException unused() {
        return new SQLException("not supported by PreparedStatment");
    }

    // 1970-01-01 00:00:00 is JD 2440587.5
    static double toJulianDay(long ms) {
        double adj = (ms < 0) ? 0 : 0.5;
        double d = (ms + adj) / 86400000.0 + 2440587.5;
        return d;
    }
}
