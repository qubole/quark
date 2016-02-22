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

package com.qubole.quark.plugins.jdbc;

import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by dev on 11/23/15.
 */
public class OracleDb extends JdbcDB {

  private final String defaultSchema = user.toUpperCase();
  private final String productName = "ORACLE";

  private final String catalogSql = "select owner, table_name, column_name, "
          + "data_type from all_tab_cols "
          + "where owner='" + user.toUpperCase() + "' "
          + "order by table_name, column_id";

  private static final ImmutableMap<String, String> DATATYPES =
      new ImmutableMap.Builder<String, String>()
          .put("NCHAR", "char")
          .put("VARCHAR2", "character varying")
          .put("NVARCHAR2", "character varying")
          .put("NUMBER", "double")
          .put("RAW", "long")
          .put("LONG RAW", "long")
          .put("BINARY_FLOAT", "float")
          .put("BINARY_DOUBLE", "double")
          .put("BLOB", "long")
          .put("CLOB", "long")
          .put("NCLOB", "long")
          .put("BFILE", "long")
          .put("TIMESTAMP\\([0-9]+\\)", "timestamp")
          .put("TIMESTAMP\\([0-9]+\\) WITH TIME ZONE", "timestamp")
          .put("TIMESTAMP\\([0-9]+\\) WITH LOCAL TIME ZONE", "timestamp")
          .put("DATE", "date")
          .put("FLOAT", "float")
          .put("CHAR", "char")
          .put("LONG", "long")
          .build();

  public OracleDb(Map<String, Object> properties) {
    super(properties);
  }

  @Override
  public Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName("oracle.jdbc.OracleDriver");
    return DriverManager.getConnection(url, user, password);
  }

  @Override
  protected String getCatalogSql() {
    return catalogSql;
  }

  @Override
  public String getDefaultSchema() {
    return defaultSchema;
  }

  @Override
  public String getProductName() {
    return productName;
  }

  @Override
  public SqlDialect getSqlDialect() {
    final SqlDialect sqlDialect = SqlDialect.getProduct("Oracle", null).getDialect();
    sqlDialect.setUseLimitKeyWord(true);
    return sqlDialect;
  }
}
