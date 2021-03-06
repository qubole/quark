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

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;

/**
 * Support AWS Redshift database as a {@link com.qubole.quark.plugin.DataSource}
 */
public class RedShiftDb extends JdbcDB {
  private final String catalogSql = "SELECT schemaname, tablename, \"column\", type "
      + "FROM pg_table_def WHERE schemaname != 'pg_catalog' ORDER BY schemaname, tablename";

  private final String defaultSchema = "PUBLIC";
  private final String productName = "REDSHIFT";

  protected static final ImmutableMap<String, Integer> DATA_TYPES =
      new ImmutableMap.Builder<String, Integer>()
          .put("STRING", Types.VARCHAR)
          .put("INTEGER", Types.INTEGER)
          .put("SMALLINT", Types.SMALLINT)
          .put("BIGINT", Types.BIGINT)
          .put("TINYINT", Types.TINYINT)
          .put("CHARACTER VARYING", Types.VARCHAR)
          .put("CHARACTER", Types.CHAR)
          .put(Primitive.BYTE.primitiveClass.getSimpleName(), Types.TINYINT)
          .put(Primitive.CHAR.primitiveClass.getSimpleName(), Types.TINYINT)
          .put(Primitive.SHORT.primitiveClass.getSimpleName(), Types.SMALLINT)
          .put(Primitive.INT.primitiveClass.getSimpleName(), Types.INTEGER)
          .put(Primitive.LONG.primitiveClass.getSimpleName(), Types.BIGINT)
          .put(Primitive.FLOAT.primitiveClass.getSimpleName(), Types.FLOAT)
          .put(Primitive.DOUBLE.primitiveClass.getSimpleName(), Types.DOUBLE)
          .put("DATE", Types.DATE)
          .put("TIME", Types.TIMESTAMP)
          .put("CHARACTER VARYING\\([0-9]+\\)", Types.VARCHAR)
          .put("TIMESTAMP WITHOUT TIME ZONE", Types.TIMESTAMP)
          .put("DOUBLE PRECISION", Types.DOUBLE)
          .put("CHARACTER\\([0-9]+\\)", Types.CHAR).build();

  public RedShiftDb(Map<String, Object> properties) {
    super(properties);
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
  protected ImmutableMap<String, Integer> getTypes(Connection connection) {
    return DATA_TYPES;
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
    final SqlDialect redshiftDialect =
        SqlDialect.getProduct("REDSHIFT", null).getDialect();
    return redshiftDialect;
  }
}
