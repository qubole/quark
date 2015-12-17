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
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by dev on 11/23/15.
 */
public class MysqlDb extends JdbcDB {

  private final String catalogSql = "select table_schema, "
      + "table_name, column_name, column_type from "
      + "information_schema.columns order by "
      + "table_schema, table_name, ordinal_position;";

  public static final ImmutableMap<String, String> DATATYPES =
      new ImmutableMap.Builder<String, String>()
        .put("int\\([0-9]+\\)", "int")
        .put("tinyint\\([0-9]+\\)", "int")
        .put("smallint\\([0-9]+\\)", "smallint")
        .put("mediumint\\([0-9]+\\)", "int")
        .put("bigint\\([0-9]+\\)", "bigint")
        .put("bigint\\([0-9]+\\)" + "( unsigned)*", "bigint")
        .put("float\\([0-9]+,[0-9]+\\)?", "float")
        .put("double\\([0-9]+,[0-9]+\\)?", "double")
        .put("decimal\\([0-9]+,[0-9]+\\)", "double")
        .put("datetime", "timestamp")
        .put("char\\([0-9]+\\)", "char")
        .put("varchar\\([0-9]+\\)", "character varying")
        .put("text", "string")
        .put("tinytext", "string")
        .put("mediumtext", "string")
        .put("longtext", "string").build();

  private String defaultSchema = null;
  private final String productName = "MYSQL";

  MysqlDb(String url, String user, String password) {
    super(url, user, password);
  }

  @Override
  public Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName("com.mysql.jdbc.Driver");
    return DriverManager.getConnection(url, user, password);
  }

  @Override
  protected String getCatalogSql() {
    return catalogSql;
  }

  @Override
  protected ImmutableMap<String, String> getDataTypes() {
    return DATATYPES;
  }

  @Override
  public String getDefaultSchema() {
    if (defaultSchema == null) {
      try {
        ResultSet rs = this.getConnection().createStatement().executeQuery("SELECT DATABASE()");
        rs.next();
        defaultSchema = rs.getString(1);
      } catch (SQLException | ClassNotFoundException e) {
        throw new RuntimeException("Couldnot fetch DefaultSchema "
                + "for mysql" + "Error:" + e.getMessage(), e);
      }
    }
    return defaultSchema;
  }

  @Override
  public String getProductName() {
    return productName;
  }

  @Override
  public SqlDialect getSqlDialect() {
    final SqlDialect sqlDialect = SqlDialect.getProduct("MySQL", null).getDialect();
    sqlDialect.setUseLimitKeyWord(true);
    return sqlDialect;
  }

  @Override
  public boolean isCaseSensitive() {
    return true;
  }
}
