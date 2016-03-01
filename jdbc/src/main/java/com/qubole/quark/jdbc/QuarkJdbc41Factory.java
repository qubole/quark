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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.jdbc.CalciteRootSchema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Implementation of {@link org.apache.calcite.avatica.AvaticaFactory} for Drill
 * and
 * JDBC 4.1 (corresponds to JDK 1.7).
 */
public class QuarkJdbc41Factory extends QuarkJdbcFactory {

  /**
   * Creates a factory for JDBC version 4.1.
   */
  public QuarkJdbc41Factory() {
    this(4, 1);
  }

  /**
   * Creates a JDBC factory with given major/minor version number.
   */
  protected QuarkJdbc41Factory(int major, int minor) {
    super(major, minor);
  }


  @Override
  protected QuarkConnectionImpl newConnection(QuarkDriver driver,
                                              QuarkJdbcFactory factory,
                                              String url,
                                              Properties info,
                                              CalciteRootSchema rootSchema,
                                              JavaTypeFactory typeFactory) throws SQLException {
    return new QuarkConnectionImpl(driver, factory, url, info, rootSchema, typeFactory);
  }

  public QuarkJdbc41DatabaseMetaData newDatabaseMetaData(
      AvaticaConnection connection) {
    return new QuarkJdbc41DatabaseMetaData(
        (QuarkConnectionImpl) connection);
  }

  @Override
  public QuarkStatement newStatement(AvaticaConnection connection,
                                     Meta.StatementHandle statementHandle,
                                     int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability) {
    return new QuarkJdbc41Statement((QuarkConnectionImpl) connection,
        statementHandle,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection,
                                                       Meta.StatementHandle h,
                                                       Meta.Signature signature,
                                                       int resultSetType,
                                                       int resultSetConcurrency,
                                                       int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public AvaticaResultSet newResultSet(AvaticaStatement statement,
                                       QueryState state,
                                       Meta.Signature signature,
                                       TimeZone timeZone,
                                       Meta.Frame firstFrame) {
    final ResultSetMetaData metaData =
        newResultSetMetaData(statement, signature);
    return new QuarkResultSet(statement, signature, metaData, timeZone,
        firstFrame);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
                                                Meta.Signature signature) {
    return new AvaticaResultSetMetaData(statement, null, signature);
  }

  /**
   * Implementation of statement for JDBC 4.1.
   */
  private static class QuarkJdbc41Statement extends QuarkStatement {
    QuarkJdbc41Statement(QuarkConnectionImpl connection,
                         Meta.StatementHandle h,
                         int resultSetType,
                         int resultSetConcurrency,
                         int resultSetHoldability) {
      super(connection, h, resultSetType, resultSetConcurrency,
          resultSetHoldability);
    }
  }

  /**
   * Implementation of database metadata for JDBC 4.1.
   */
  private static class QuarkJdbc41DatabaseMetaData
      extends AvaticaDatabaseMetaData {
    QuarkJdbc41DatabaseMetaData(QuarkConnectionImpl connection) {
      super(connection);
    }
  }

}
