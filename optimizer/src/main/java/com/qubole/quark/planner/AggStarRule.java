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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.TileKey;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Rule to replace an aggregation with a materialized tile.
 * The rule also adds a filter on the grouping_id column.
 */
public class AggStarRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(AggStarRule.class);

  public static final AggStarRule INSTANCE =
      new AggStarRule(
          operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
              some(operand(StarTable.StarTableScan.class, none()))),
          "AggStarRule");

  public static final AggStarRule INSTANCE2 =
      new AggStarRule(
          operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
              operand(Project.class,
                  operand(StarTable.StarTableScan.class, none()))),
          "AggStarRule:project") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final Aggregate aggregate = call.rel(0);
          final Project project = call.rel(1);
          final StarTable.StarTableScan scan = call.rel(2);
          final RelNode rel =
              AggregateProjectMergeRule.apply(call, aggregate, project);
          final Aggregate aggregate2;
          final Project project2;
          if (rel instanceof Aggregate) {
            project2 = null;
            aggregate2 = (Aggregate) rel;
          } else if (rel instanceof Project) {
            project2 = (Project) rel;
            aggregate2 = (Aggregate) project2.getInput();
          } else {
            return;
          }
          apply(call, project2, aggregate2, scan);
        }
      };

  private AggStarRule(RelOptRuleOperand operand,
                      String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final StarTable.StarTableScan scan = call.rel(1);
    apply(call, null, aggregate, scan);
  }

  protected void apply(RelOptRuleCall call, Project postProject,
                       final Aggregate aggregate, StarTable.StarTableScan scan) {
    final RelOptCluster cluster = scan.getCluster();
    final RelOptTable table = scan.getTable();
    final RelOptLattice lattice = call.getPlanner().getLattice(table);
    if (lattice.lattice.filter != null) {
      return;
    }
    final List<Lattice.Measure> measures =
        lattice.lattice.toMeasures(aggregate.getAggCallList());
    final Pair<CalciteSchema.TableEntry, TileKey> pair =
        lattice.getAggregate(call.getPlanner(), aggregate.getGroupSet(),
            measures);
    if (pair == null) {
      return;
    }
    final CalciteSchema.TableEntry tableEntry = pair.left;
    final TileKey tileKey = pair.right;
    final double rowCount = aggregate.getRows();
    final QuarkTileTable aggregateTable = (QuarkTileTable) tableEntry.getTable();

    final RelDataType aggregateTableRowType =
        aggregateTable.getRowType(cluster.getTypeFactory());
    final RelOptTable aggregateRelOptTable =
        RelOptTableImpl.create(table.getRelOptSchema(), aggregateTableRowType,
            tableEntry, rowCount);
    RelNode rel = aggregateRelOptTable.toRel(RelOptUtil.getContext(cluster));

    // Aggregate has finer granularity than we need. Roll up.
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Using materialization "
          + aggregateRelOptTable.getQualifiedName()
          + ", rolling up " + tileKey.dimensions + " to "
          + aggregate.getGroupSet());
    }
    assert tileKey.dimensions.contains(aggregate.getGroupSet());
    final List<AggregateCall> aggCalls = Lists.newArrayList();
    ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
    for (int key : aggregate.getGroupSet()) {
      groupSet.set(tileKey.dimensions.indexOf(key));
    }

    //Create a filter

    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    List<RexNode> filterArgs = Lists.newArrayList();
    filterArgs.add(rexBuilder.makeInputRef(rel, aggregateTable.quarkTile.groupingColumn));
    filterArgs.add(rexBuilder.makeLiteral(bitSetToString(aggregateTable.quarkTile.groupingValue)));

    rel = LogicalFilter.create(rel, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, filterArgs));

    //Create a project list
    List<Integer> posList = Lists.newArrayList();
    for (QuarkTile.Column quarkColumn : aggregateTable.quarkTile.cubeColumns) {
      posList.add(quarkColumn.cubeOrdinal);
    }

    for (Lattice.Measure measure : aggregateTable.quarkTile.measures) {
      for (Lattice.Measure m : measures) {
        if (m.equals(measure)) {
          posList.add(((QuarkTile.Measure) measure).ordinal);
        }
      }
    }

    rel = RelOptUtil.createProject(rel, posList);

    int index = aggregateTable.quarkTile.cubeColumns.size();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      final AggregateCall copy = AggregateCall.create(aggCall.getAggregation(),
          false, ImmutableList.of(index), -1, groupSet.cardinality(), rel, null, aggCall.name);
      aggCalls.add(copy);
      index++;
    }

    rel = aggregate.copy(aggregate.getTraitSet(), rel, false,
        groupSet.build(), null, aggCalls);

    if (postProject != null) {
      rel = postProject.copy(postProject.getTraitSet(), ImmutableList.of(rel));
    }
    call.transformTo(rel);
  }

  String bitSetToString(ImmutableBitSet bits) {
    long result = 0;
    for (Integer i : bits) {
      result += LongMath.checkedPow(2, i);
    }

    return String.valueOf(result);
  }
}
