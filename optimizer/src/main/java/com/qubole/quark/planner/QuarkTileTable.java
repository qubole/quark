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

import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rajatv on 6/22/15.
 */
public class QuarkTileTable extends AbstractTable
    implements QueryableTable, TranslatableTable {
  protected static final Logger LOG = LoggerFactory.getLogger(QuarkTable.class);

  protected final QuarkTable backingTable;
  protected final RelOptTable relOptTable;
  protected final QuarkTile quarkTile;

  public QuarkTileTable(QuarkTile quarkTile, CalciteCatalogReader calciteCatalogReader,
                        RelDataType relDataType, Path path, QuarkTable backingTable) {
    this.quarkTile = quarkTile;
    this.backingTable = backingTable;
    this.relOptTable = RelOptTableImpl.create(
        calciteCatalogReader,
        relDataType,
        this,
        path);
  }

  public QuarkTileTable(QuarkTile quarkTile, RelOptTable relOptTable, QuarkTable backingTable) {
    this.quarkTile = quarkTile;
    this.backingTable = backingTable;
    this.relOptTable = relOptTable;
  }

  /**
   * Returns an enumerable over a given projection of the fields.
   *
   * Called from generated code.
   */
  public Enumerable<Object> project(final int[] fields) {
    return backingTable.project(fields);
  }

  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public Type getElementType() {
    return Object[].class;
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    RelNode rel = new QuarkTileScan(context.getCluster(),
        this.relOptTable, this.quarkTile, this.backingTable);

    //Create a filter

    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    List<RexNode> filterArgs = Lists.newArrayList();
    filterArgs.add(rexBuilder.makeInputRef(rel, this.quarkTile.groupingColumn));
    filterArgs.add(rexBuilder.makeLiteral(bitSetToString(this.quarkTile.groupingValue)));

    rel = LogicalFilter.create(rel, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, filterArgs));

    //Create a project list
    List<Integer> posList = Lists.newArrayList();
    for (QuarkTile.Column quarkColumn : this.quarkTile.cubeColumns) {
      posList.add(quarkColumn.cubeOrdinal);
    }

    for (Lattice.Measure measure : this.quarkTile.measures) {
      posList.add(((QuarkTile.Measure) measure).ordinal);
    }

    return RelOptUtil.createProject(rel, posList);

  }

  String bitSetToString(ImmutableBitSet bits) {
    long result = 0;
    for (Integer i : bits) {
      result += LongMath.checkedPow(2, i);
    }

    return String.valueOf(result);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<QuarkColumn> columns = this.backingTable.columns;
    final List<String> names = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();

    for (QuarkTile.Column quarkColumn : this.quarkTile.cubeColumns) {
      addColumn(columns.get(quarkColumn.cubeOrdinal), names, types, typeFactory);
    }

    for (Lattice.Measure measure : this.quarkTile.measures) {
      addColumn(columns.get(((QuarkTile.Measure) measure).ordinal), names, types, typeFactory);
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  private void addColumn(QuarkColumn column, List<String> names, List<RelDataType> types,
                         RelDataTypeFactory typeFactory) {
    final FieldType fieldType = FieldType.of(column.type);
    if (fieldType == null) {
      LOG.error("Field Type is null for " + column.type);
    }

    final RelDataType type = fieldType.toType((JavaTypeFactory) typeFactory);
    types.add(type);
    names.add(column.name);
  }

  public Statistic getStatistic() {
    return Statistics.of(0d, ImmutableList.<ImmutableBitSet>of());
  }
}
