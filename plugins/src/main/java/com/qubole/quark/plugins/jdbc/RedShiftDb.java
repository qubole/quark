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
import java.util.Properties;

/**
 * Support AWS Redshift database as a {@link com.qubole.quark.plugin.DataSource}
 */
public class RedShiftDb extends JdbcDB {
  private final String catalogSql = "SELECT schemaname, tablename, \"column\", type "
      + "FROM pg_table_def WHERE schemaname != 'pg_catalog' ORDER BY schemaname, tablename";

  private final String defaultSchema = "PUBLIC";
  private final String productName = "REDSHIFT";

  public static final ImmutableMap<String, String> DATATYPES =
      new ImmutableMap.Builder<String, String>()
        .put("character varying\\([0-9]+\\)", "character varying")
        .put("timestamp without time zone", "timestamp")
        .put("double precision", "float")
        .put("character\\([0-9]+\\)", "character").build();

  RedShiftDb(String url, String user, String password) {
    super(url, user, password);
  }

  public Connection getConnection() throws ClassNotFoundException, SQLException {
    //TODO : check if there is a better way for this as this conflicts with QuarkDriver
    Class.forName("org.postgresql.Driver");
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    return DriverManager.getConnection(url, props);
  }

  @Override
  public String getCatalogSql() {
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
  public ImmutableMap<String, String> getDataTypes() {
    return DATATYPES;
  }

  @Override
  public SqlDialect getSqlDialect() {
    final SqlDialect redshiftDialect =
        SqlDialect.getProduct("REDSHIFT", null).getDialect();
    redshiftDialect.setUseLimitKeyWord(true);
    return redshiftDialect;
  }
}