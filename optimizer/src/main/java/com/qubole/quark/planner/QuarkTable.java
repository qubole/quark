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

package com.qubole.quark.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Source of inspiration is Kylin:OlapTable
 * and
 * https://github.com/apache/incubator-calcite/tree/master/
 * example/csv/src/main/java/org/apache/calcite/adapter/csv
 */
public class QuarkTable extends AbstractTable
    implements QueryableTable, TranslatableTable {

  protected static final Logger LOG = LoggerFactory.getLogger(QuarkTable.class);

  protected final QuarkSchema schema;
  protected final String name;
  protected final List<QuarkColumn> columns;

  public QuarkTable(QuarkSchema schema, String name, List<QuarkColumn> columns) {
    this.schema = schema;
    this.name = name;
    this.columns = columns;
  }

  /**
   * Returns an enumerable over a given projection of the fields.
   *
   * Called from generated code.
   */
  public Enumerable<Object> project(final int[] fields) {
    return new AbstractEnumerable<Object>() {
      public org.apache.calcite.linq4j.Enumerator enumerator() {
        return new QuarkEnumerator();
      }
    };
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    return new QuarkTableScan(context.getCluster(), relOptTable, this);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final List<String> names = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();
    for (QuarkColumn col : this.columns) {
      final FieldType fieldType = FieldType.of(col.type);
      if (fieldType == null) {
        LOG.error("Field Type is null for " + col.type);
      }
      final RelDataType type = fieldType.toType((JavaTypeFactory) typeFactory);
      types.add(type);
      names.add(col.name);
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  public List<QuarkColumn> getColumns() {
    return columns;
  }

  public int getFieldOrdinal(String columnName) {
    int count = 0;
    for (QuarkColumn column : columns) {
      if (columnName.equals(column.name)) {
        return count;
      }
      count++;
    }

    throw new RuntimeException("Column " + columnName + " not found in " + this.toString());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (this.getClass() != obj.getClass()) return false;
    QuarkTable other = (QuarkTable) obj;
    return (schema.equals(other.schema) && name.equals(other.name) && columns.equals(other.columns));
  }

  @Override
  public int hashCode() {
    return schema.hashCode() + name.hashCode() * 31 + columns.hashCode() * 47;
  }
}
