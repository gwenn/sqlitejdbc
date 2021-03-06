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

import java.sql.SQLException;
import java.sql.Savepoint;

class Spt implements Savepoint {
    private final int id;
    private final String name;

    Spt(int id) {
        this.id = id;
        this.name = null;
    }

    Spt(String name) {
        if (null == name || 0 == name.length()) { // TODO Validate
            throw new IllegalArgumentException("Invalid savepoint name");
        }
        this.name = name;
        this.id = 0;
    }

    public int getSavepointId() throws SQLException {
      if (null != name) {
          throw new SQLException("Named savepoint");
      }
      return id;
    }
    public String getSavepointName() throws SQLException {
      if (null == name) {
          throw new SQLException("Unamed savepoint");
      }
      return name;
    }

    String getNameOrId() {
      if (null == name) {
          return "JDBC_SAVEPOINT_" + id;
      }
      return name;
    }
}
