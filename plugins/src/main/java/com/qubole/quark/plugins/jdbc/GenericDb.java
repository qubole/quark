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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * Support Generic database
 */
public class GenericDb extends JdbcDB {

  private Map<String, Object> properties;
  private String catalogSql = null;
  private String defaultSchema = null;
  private String productName = null;
  private boolean caseSensitive = false;

  public GenericDb(Map<String, Object> properties) {
    super(properties);
    this.properties = properties;
    defaultSchema = (String) properties.get("defaultSchema");
    catalogSql = (String) properties.get("catalogSql");
    productName = (String) properties.get("productName");
    caseSensitive = properties.get("isCaseSensitive").toString().equalsIgnoreCase("true");
  }

  @Override
  public Connection getConnection() throws ClassNotFoundException, SQLException {
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
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public SqlDialect getSqlDialect() {
    final SqlDialect sqlDialect = SqlDialect.getProduct((String) properties.get("type"), null)
        .getDialect();
    return sqlDialect;
  }
}
