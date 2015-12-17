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

package com.qubole.quark.plugins.qubole;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.qubole.qds.sdk.java.client.DefaultQdsConfiguration;
import com.qubole.qds.sdk.java.client.QdsClient;
import com.qubole.qds.sdk.java.client.QdsClientFactory;
import com.qubole.qds.sdk.java.client.QdsConfiguration;
import com.qubole.qds.sdk.java.entities.NameAndType;
import com.qubole.qds.sdk.java.entities.NameTypePosition;
import com.qubole.qds.sdk.java.entities.ResultValue;
import com.qubole.qds.sdk.java.entities.SchemaOrdinal;

import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.QuarkColumn;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.plugins.Executor;
import com.qubole.quark.plugins.SimpleSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by dev on 11/13/15.
 */
public abstract class QuboleDB implements Executor {
  private static final Logger LOG = LoggerFactory.getLogger(QuboleDB.class.getName());
  private static final String ROW_DELIMETER = "\r\n";
  private static final String COLUMN_DELIMETER = "\t";
  protected abstract Map<String, List<SchemaOrdinal>>
                        getSchemaListDescribed() throws ExecutionException, InterruptedException;
  protected abstract ImmutableMap<String, String> getDataTypes();
  public abstract boolean isCaseSensitive();

  protected String token;
  protected String endpoint;

  QuboleDB(String endpoint, String token) {
    this.endpoint = endpoint;
    this.token = token;
  }

  @Override
  public void cleanup() throws Exception {
  }

  protected QdsClient getQdsClient() {
    QdsConfiguration configuration = new DefaultQdsConfiguration(endpoint, token);
    return QdsClientFactory.newClient(configuration);
  }

  @Override
  public ImmutableMap<String, Schema> getSchemas() throws QuarkException {

    ImmutableMap<String, String> dataTypes = this.getDataTypes();

    Map<String, List<SchemaOrdinal>> schemas = null;
    try {
      schemas = getSchemaListDescribed();
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Getting Schema metadata for " + this.endpoint
          + " failed. Error: " + e.getMessage(), e);
      throw new QuarkException(e.getCause());
    }
    ImmutableMap.Builder<String, Schema> schemaBuilder = new ImmutableMap.Builder<>();

    for (String schemaName: schemas.keySet()) {
      ImmutableMap.Builder<String, Table> tableBuilder = new ImmutableMap.Builder<>();
      List<SchemaOrdinal> listOfTables = null;
      try {
        listOfTables = schemas.get(schemaName);
      } catch (Exception e) {
        LOG.warn("Could not add schema " + schemaName + " for datasource: "
            + this.endpoint, e);
        continue;
      }
      for (SchemaOrdinal table: listOfTables) {
        List<NameTypePosition> columns = table.getColumns();
        Collections.sort(columns, new ColumnComparator());
        ImmutableList.Builder<QuarkColumn> columnBuilder =
            new ImmutableList.Builder<>();
        for (NameAndType column : columns) {
          String dataType = column.getType();
          for (String key : dataTypes.keySet()) {
            if (dataType.matches(key)) {
              dataType = dataTypes.get(key);
              break;
            }
          }
          String columnName = column.getName();
          if (!this.isCaseSensitive()) {
            columnName = columnName.toUpperCase();
          }
          columnBuilder.add(new QuarkColumn(columnName, dataType));
        }
        String tableName = table.getTable_name();
        if (!this.isCaseSensitive()) {
          tableName = tableName.toUpperCase();
        }
        tableBuilder.put(tableName, new QuarkTable(columnBuilder.build()));
      }

      if (!this.isCaseSensitive()) {
        schemaName = schemaName.toUpperCase();
      }
      schemaBuilder.put(schemaName, new SimpleSchema(schemaName, tableBuilder.build()));
    }
    return schemaBuilder.build();
  }

  protected Iterator<Object> convertToIterator(ResultValue resultValue) {
    String[] result = resultValue.getResults().split(ROW_DELIMETER);
    List<Object> resultSet = new ArrayList<>();

    for (int i = 0; i < result.length; i++) {
      //check if Empty Result Set
      // QDS returns row of empty column
      if (result[i].length() == 0) {
        resultSet = new ArrayList<>();
        break;
      }
      String[] row = result[i].split(COLUMN_DELIMETER);
      resultSet.add(row);
    }
    return resultSet.iterator();
  }

  /**
   * Created by dev on 11/13/15.
   */
  class ColumnComparator implements Comparator<NameTypePosition> {
    @Override
    public int compare(NameTypePosition col1, NameTypePosition col2) {
      return Integer.parseInt(col1.getOrdinal_position())
              - (Integer.parseInt(col2.getOrdinal_position()));
    }
  }
}
