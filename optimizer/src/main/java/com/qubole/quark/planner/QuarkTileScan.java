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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * A scan operator for a {@link QuarkTileTable}
 */
public class QuarkTileScan extends TableScan implements EnumerableRel {
  final QuarkTable quarkTable;
  final QuarkTile quarkTile;

  protected QuarkTileScan(RelOptCluster cluster, RelOptTable table, QuarkTile quarkTile,
                          QuarkTable quarkTable) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
    this.quarkTile = quarkTile;
    this.quarkTable = quarkTable;
    assert quarkTable != null;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new QuarkTileScan(getCluster(), table, quarkTile, quarkTable);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("fields", table.getRowType());
  }

  @Override
  public RelDataType deriveRowType() {
    return table.getRowType();
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(ProjectRule.INSTANCE);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray());

    return implementor.result(
        physType,
        Blocks.toBlock(
            Expressions.call(table.getExpression(QuarkTileTable.class),
                "project", Expressions.constant(
                    QuarkEnumerator.identityList(getRowType().getFieldCount())))));
  }

  public QuarkTable getQuarkTable() {
    return quarkTable;
  }

  public QuarkTile getQuarkTile() {
    return quarkTile;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeZeroCost();
  }
}
