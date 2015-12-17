/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.jdbc;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;

/**
 * Implementation of {@link java.sql.Statement}
 * for the Calcite engine.
 */
public abstract class QuarkStatement extends AvaticaStatement {
  /**
   * Creates a QuarkStatement.
   *
   * @param connection           Connection
   * @param h                    Statement handle
   * @param resultSetType        Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   */
  QuarkStatement(QuarkConnectionImpl connection, Meta.StatementHandle h,
                 int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    super(connection, h, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  // implement Statement

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == QuarkJdbcStatement.class) {
      return iface.cast(getConnection().server.getStatement(handle));
    }
    return super.unwrap(iface);
  }

  @Override
  public QuarkConnectionImpl getConnection() {
    return (QuarkConnectionImpl) connection;
  }

  @Override
  protected void close_() {
    if (!closed) {
      closed = true;
      final QuarkConnectionImpl connection1 =
          (QuarkConnectionImpl) connection;
      connection1.server.removeStatement(handle);
      if (openResultSet != null) {
        AvaticaResultSet c = openResultSet;
        openResultSet = null;
        c.close();
      }
      // If onStatementClose throws, this method will throw an exception (later
      // converted to SQLException), but this statement still gets closed.
      connection1.getDriver().handler.onStatementClose(this);
    }
  }
}
