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

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

/**
 * Table that is a materialized view.
 * <p/>
 * <p>It can exist in two states: materialized and not materialized. Over time,
 * a given materialized view may switch states. How it is expanded depends upon
 * its current state. State is managed by
 * {@link org.apache.calcite.materialize.MaterializationService}.</p>
 */
public class QuarkViewTable extends QuarkTable {

  private final String name;
  private final RelOptTableImpl backUpRelOptTable; // This is of the backup table
  private final QuarkTable backupTable;
  private final CalciteSchema backupTableSchema;

  public QuarkViewTable(String name,
                        RelOptTableImpl relOptTable,
                        QuarkTable backupTable,
                        CalciteSchema tableSchema) {
    super(backupTable.getColumns());
    this.name = name;
    this.backUpRelOptTable = relOptTable;
    this.backupTable = backupTable;
    this.backupTableSchema = tableSchema;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return backupTable.getRowType(typeFactory);
  }

  public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      @SuppressWarnings("unchecked")
      public Enumerator<T> enumerator() {
        return null;
      }
    };
  }

  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return null;
      }
    };
  }

  public Statistic getStatistic() {
    return Statistics.of(0d, ImmutableList.<ImmutableBitSet>of());
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new QuarkViewScan(context.getCluster(), this.backUpRelOptTable, this);
  }

  public CalciteSchema getBackupTableSchema() {
    return this.backupTableSchema;
  }

}
// End MaterializedViewTable.java
