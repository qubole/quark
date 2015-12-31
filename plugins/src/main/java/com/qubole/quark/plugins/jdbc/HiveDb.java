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
 * Support Apache Hive database as a {@link com.qubole.quark.plugin.DataSource}
 */
public class HiveDb extends JdbcDB {
  /* Union can distort the ordering of individual select even if they have order by constraint
   * So adding union_sort to have column names always before partitions
   * And sorting each of them using their index for every Table in a particular JdbcDB.
   */
  private final String catalogSql = "SELECT nm, tbl, cn, tn FROM "
      + "((SELECT d.name as nm, t.tbl_name as tbl, c.column_name as cn, "
      + "c.type_name as tn, c.integer_idx as idx, "
      + "1 as union_sort FROM DBS d INNER JOIN TBLS t ON t.db_id = d.db_ID "
      + "INNER JOIN SDS s ON s.SD_ID = t.SD_ID "
      + "INNER JOIN  COLUMNS_V2 c ON c.CD_ID = s.CD_ID) "
      + "UNION "
      + "(SELECT d.name as nm, t.tbl_name as tbl, k.pkey_name as cn, "
      + "k.pkey_type as tn, k.integer_idx as idx, "
      + "2 as union_sort FROM DBS d INNER JOIN TBLS t ON t.db_id = d.db_ID "
      + "INNER JOIN PARTITION_KEYS k ON (t.TBL_ID=k.TBL_ID))) res "
      + "ORDER BY nm, tbl, union_sort, idx asc;";

  private final String defaultSchema = "DEFAULT";
  private final String productName = "HIVE";

  private static final ImmutableMap<String, String> DATA_TYPES =
      new ImmutableMap.Builder<String, String>()
        .put("character varying\\([0-9]+\\)", "character varying")
        .put("varchar\\([0-9]+\\)", "character varying")
        .put("char\\([0-9]+\\)", "character")
        .put("character\\([0-9]+\\)", "character")
        .put("decimal\\([0-9]+,[0-9]+\\)", "double").build();

  public HiveDb(Map<String, Object> properties) {
    super(properties);
  }

  public Connection getConnection() throws ClassNotFoundException, SQLException {
    return DriverManager.getConnection(url, user, password);
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
    return DATA_TYPES;
  }

  @Override
  public SqlDialect getSqlDialect() {
    final SqlDialect hiveDialect =
        SqlDialect.getProduct("Hive", null).getDialect();
    hiveDialect.setUseLimitKeyWord(true);
    return hiveDialect;
  }
}
