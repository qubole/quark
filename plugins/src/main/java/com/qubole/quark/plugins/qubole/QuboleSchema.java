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

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qubole.qds.sdk.java.entities.NameAndType;
import com.qubole.qds.sdk.java.entities.NameTypePosition;
import com.qubole.qds.sdk.java.entities.SchemaOrdinal;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.QuarkColumn;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.sql.QueryContext;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Created by rvenkatesh on 7/10/17.
 */
public class QuboleSchema extends QuarkSchema {
  protected static final ImmutableMap<String, Integer> DATA_TYPES =
      new ImmutableMap.Builder<String, Integer>()
          .put("string", Types.VARCHAR)
          .put("character varying", Types.VARCHAR)
          .put("character", Types.CHAR)
          .put("integer", Types.INTEGER)
          .put("smallint", Types.SMALLINT)
          .put("bigint", Types.BIGINT)
          .put("tinyint", Types.TINYINT)
          .put(Primitive.BYTE.primitiveClass.getSimpleName(), Types.TINYINT)
          .put(Primitive.CHAR.primitiveClass.getSimpleName(), Types.TINYINT)
          .put(Primitive.SHORT.primitiveClass.getSimpleName(), Types.SMALLINT)
          .put(Primitive.INT.primitiveClass.getSimpleName(), Types.INTEGER)
          .put(Primitive.LONG.primitiveClass.getSimpleName(), Types.BIGINT)
          .put(Primitive.FLOAT.primitiveClass.getSimpleName(), Types.FLOAT)
          .put(Primitive.DOUBLE.primitiveClass.getSimpleName(), Types.DOUBLE)
          .put("date", Types.DATE)
          .put("time", Types.TIMESTAMP)
          .put("boolean", Types.BOOLEAN).build();


  private final ImmutableMap<String, Table> tableMap;

  public QuboleSchema(String name, List<SchemaOrdinal> tables,
                      boolean isCaseSensitive,
                      ImmutableMap<String, Integer> dataTypes) throws QuarkException {
    super(isCaseSensitive ? name : name.toUpperCase());
    ImmutableMap.Builder<String, Table> tableBuilder = new ImmutableMap.Builder<>();
    for (SchemaOrdinal table : tables) {
      List<NameTypePosition> columns = table.getColumns();
      columns.sort(new ColumnComparator());
      ImmutableList.Builder<QuarkColumn> columnBuilder = new ImmutableList.Builder<>();
      for (NameAndType column : columns) {
        String columnName = column.getName();
        if (!isCaseSensitive) {
          columnName = columnName.toUpperCase();
        }
        columnBuilder.add(new QuarkColumn(columnName, getType(column.getType(), dataTypes)));
      }
      String tableName = table.getTable_name();
      if (!isCaseSensitive) {
        tableName = tableName.toUpperCase();
      }
      tableBuilder.put(tableName, new QuarkTable(this, tableName, columnBuilder.build()));
    }

    tableMap = tableBuilder.build();
  }

  private Integer getType(String dataType, ImmutableMap<String, Integer> dataTypes)
      throws QuarkException {
    if (DATA_TYPES.containsKey(dataType)) {
      return DATA_TYPES.get(dataType);
    } else {
      for (String key : dataTypes.keySet()) {
        if (dataType.matches(key)) {
          return dataTypes.get(key);
        }
      }
    }

    throw new QuarkException("Unknown DataType `" + dataType + "`");
  }

  @Override
  public void initialize(QueryContext queryContext) {}

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
