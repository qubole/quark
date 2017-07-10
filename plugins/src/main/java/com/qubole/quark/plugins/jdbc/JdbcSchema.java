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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qubole.quark.planner.QuarkColumn;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.sql.QueryContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by rvenkatesh on 7/10/17.
 */
public class JdbcSchema extends QuarkSchema {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSchema.class);
  private final ImmutableMap<String, Table> tableMap;

  public JdbcSchema(String name, ResultSet resultSet, boolean isCaseSensitive,
                    ImmutableMap<String, Integer> dataTypes) throws SQLException {
    super(isCaseSensitive ? name : name.toUpperCase());

    ImmutableMap.Builder<String, Table> tableBuilder = new ImmutableMap.Builder<>();
    while (!resultSet.isAfterLast() && resultSet.getString(1).equals(name)) {
      ImmutableList.Builder<QuarkColumn> columnBuilder = new ImmutableList.Builder<>();
      String currentTable = resultSet.getString(2);
      while (resultSet.getString(2).equals(currentTable)) {
        String columnName = resultSet.getString(3);
        if (!isCaseSensitive) {
          columnName = columnName.toUpperCase();
        }
        Integer dataType = null;
        for (String key: dataTypes.keySet()) {
          if (resultSet.getString(4).toUpperCase().matches(key)) {
            dataType = dataTypes.get(key);
            break;
          }
        }

        if (dataType == null) {
          throw new SQLException("DataType `" + resultSet.getString(4) + "` is not supported");
        }

        columnBuilder.add(new QuarkColumn(columnName, dataType));
        LOG.debug("Adding column:  " + resultSet.getString(1) + " : "
            + resultSet.getString(2) + " : "
            + resultSet.getString(3) + " : "
            + resultSet.getString(4));
        if (!resultSet.next()) {
          break;
        }
      }

      if (!isCaseSensitive) {
        currentTable = currentTable.toUpperCase();
      }
      tableBuilder.put(currentTable, new QuarkTable(this, currentTable, columnBuilder.build()));
    }

    tableMap = tableBuilder.build();
  }

  @Override
  public void initialize(QueryContext queryContext, SchemaPlus schemaPlus) {
    this.schemaPlus = schemaPlus;
    for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
      this.schemaPlus.add(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
