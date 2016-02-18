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
import java.util.Properties;

/**
 * Support H2 database as a {@link com.qubole.quark.plugin.DataSource}
 */
public class H2Db extends JdbcDB {
  private final String catalogSql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, TYPE_NAME "
      + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' "
      + "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

  private final String defaultSchema = "PUBLIC";
  private final String productName = "H2";

  private static final ImmutableMap<String, String> DATA_TYPES =
      new ImmutableMap.Builder<String, String>()
        .put("integer", "integer")
        .put("character varying", "character varying")
        .put("-6", "smallint")
        .put("-5", "integer")
        .put("3", "double")
        .put("4", "integer")
        .put("5", "smallint")
        .put("8", "double")
        .put("12", "character varying")
        .put("93", "timestamp")
        .put("16", "boolean")
        .put("1", "character")
        .put("91", "date").build();

  public H2Db(Map<String, Object> properties) {
    super(properties);
  }

  public Connection getConnection() throws ClassNotFoundException, SQLException {
    //TODO : check if there is a better way for this as this conflicts with QuarkDriver
    Class.forName("org.h2.Driver");
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
  public SqlDialect getSqlDialect() {
    final SqlDialect h2Dialect =
        SqlDialect.getProduct("H2", null).getDialect();
    h2Dialect.setUseLimitKeyWord(true);
    return h2Dialect;
  }
}
