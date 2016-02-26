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
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.google.common.math.LongMath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Rule to replace an filtered aggregation with a materialized tile.
 * The rule also adds a filter on the grouping_id column.
 */
public class FilterAggStarRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(FilterAggStarRule.class);

  public static final FilterAggStarRule INSTANCE =
      new FilterAggStarRule(
          operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
              operand(LogicalFilter.class,
                  operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
                      operand(Project.class,
                          operand(StarTable.StarTableScan.class, none()))))),
          "FilterAggStarRule");

  public static final FilterAggStarRule INSTANCE2 =
      new FilterAggStarRule(
          operand(LogicalFilter.class,
              operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
                  operand(Project.class,
                      operand(StarTable.StarTableScan.class, none())))),
          "FilterAggStarRule:FilterOnGroupSet") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          final LogicalFilter filter = call.rel(0);
          Aggregate aggregate2 = call.rel(1);
          final Project project = call.rel(2);
          final StarTable.StarTableScan scan = call.rel(3);
          apply(call, null, filter, aggregate2, project, scan);
        }
      };
  private FilterAggStarRule(RelOptRuleOperand operand,
                            String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate1 = call.rel(0);
    final LogicalFilter filter = call.rel(1);
    Aggregate aggregate2 = call.rel(2);
    final Project project = call.rel(3);
    final StarTable.StarTableScan scan = call.rel(4);
    apply(call, aggregate1, filter, aggregate2, project, scan);
  }

  protected void apply(RelOptRuleCall call, Aggregate aggregate1, LogicalFilter filter,
      Aggregate aggregate2, Project project, StarTable.StarTableScan scan) {
    final RelOptLattice lattice = call.getPlanner().getLattice(scan.getTable());

    //STEP 1: Merge rels 2 and 3 to create an Aggregate.
    // After this tree would look like:  Aggregate -> Filter -> Aggregate ->Scan
    final RelNode mergeAggProj =
        AggregateProjectMergeRule.apply(call, aggregate2, project);

    if (!(mergeAggProj instanceof Aggregate)) {
      return;
    }

    //STEP 2: Push the filter past new Aggregate created in STEP 1
    // After this tree would look like:  Aggregate -> Aggregate -> Filter ->Scan
    final LogicalFilter pushedDownfilter;
    RelNode filterPushedDown = filterAggregateTranspose(call, filter, (Aggregate) mergeAggProj);
    if (filterPushedDown == null || !(filterPushedDown instanceof Aggregate)) {
      return;
    }
    pushedDownfilter = (LogicalFilter) ((Aggregate) filterPushedDown).getInput();
    if (!isLatticeFilterSatisfied(lattice, pushedDownfilter, scan)) {
      return;
    }

    //STEP 3: Merge the top 2 Aggregate
    // After this tree would look like: Aggregate -> Filter -> Scan
    Aggregate mergedAggregate = (Aggregate) mergeAggProj;
    if (aggregate1 != null) {
      mergedAggregate = mergeAggregate(aggregate1, (Aggregate) mergeAggProj);
    }
    if (mergedAggregate == null) {
      return;
    }

    //STEP 4: Calculate measures and tiles required
    final List<Lattice.Measure> measures =
        lattice.lattice.toMeasures(mergedAggregate.getAggCallList());
    final Pair<CalciteSchema.TableEntry, TileKey> pair =
        lattice.getAggregate(call.getPlanner(), mergedAggregate.getGroupSet(),
            measures);
    if (pair == null) {
      return;
    }
    final CalciteSchema.TableEntry tableEntry = pair.left;
    final QuarkTileTable aggregateTable = (QuarkTileTable) tableEntry.getTable();
    final RelDataType aggregateTableRowType =
        aggregateTable.getRowType(scan.getCluster().getTypeFactory());
    final double rows = (aggregate1 == null) ? aggregate2.getRows() : aggregate1.getRows();
    final RelOptTable aggregateRelOptTable =
        RelOptTableImpl.create(scan.getTable().getRelOptSchema(),
            aggregateTableRowType,
            tableEntry, rows);

    //STEP 5: Once we have tile figure out the transform the filter condition on Star scan to
    // filter condition on Tile. For that we need to first ensured filter is only on dimensions.
    // Filters on non-dimensions cannot be converted to tile.
    final QuarkTile quarkTile = aggregateTable.quarkTile;
    ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(pushedDownfilter.getCondition());
    ImmutableBitSet dims = ImmutableBitSet.of(quarkTile.dimensionToCubeColumn.keySet());
    if (!dims.contains(rCols)) {
      return;
    }
    final int[] adjustments = new int[scan.getRowType().getFieldList().size()];
    for (Map.Entry<Integer, Integer> e : quarkTile.dimensionToCubeColumn.entrySet()) {
      adjustments[e.getKey()] = e.getValue() - e.getKey();
    }
    RexNode filterConditionOnTile = pushedDownfilter.getCondition().accept(
        new RelOptUtil.RexInputConverter(scan.getCluster().getRexBuilder(),
            scan.getRowType().getFieldList(),
            aggregateRelOptTable.getRowType().getFieldList(),
            adjustments));

    //STEP 6: Construct the final rel on the Tile
    RelNode rel = constructTileRel(scan, mergedAggregate, measures,
        aggregateRelOptTable, filterConditionOnTile, quarkTile);

    call.transformTo(rel);
  }

  private RelNode constructTileRel(final StarTable.StarTableScan scan,
                                   final Aggregate mergedAggregate,
                                   final List<Lattice.Measure> measures,
                                   final RelOptTable aggregateRelOptTable,
                                   final RexNode filterConditionOnTile,
                                   final QuarkTile quarkTile) {

    RelNode rel = aggregateRelOptTable.toRel(RelOptUtil.getContext(scan.getCluster()));

    // Aggregate has finer granularity than we need. Roll up.
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Using materialization "
          + aggregateRelOptTable.getQualifiedName()
          + ", rolling up " + quarkTile.bitSet() + " to "
          + mergedAggregate.getGroupSet());
    }
    assert quarkTile.bitSet().contains(mergedAggregate.getGroupSet());
    final List<AggregateCall> aggCalls = Lists.newArrayList();
    ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
    for (int key : mergedAggregate.getGroupSet()) {
      groupSet.set(quarkTile.bitSet().indexOf(key));
    }

    //Create a filter on tile
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    List<RexNode> filterArgs = Lists.newArrayList();
    filterArgs.add(rexBuilder.makeInputRef(rel, quarkTile.groupingColumn));
    filterArgs.add(rexBuilder.makeLiteral(bitSetToString(quarkTile.groupingValue)));

    rel = LogicalFilter.create(rel,
        RexUtil.composeConjunction(rexBuilder, ImmutableList.of(filterConditionOnTile,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, filterArgs)), true));

    //Create a project list
    List<Integer> posList = Lists.newArrayList();
    for (QuarkTile.Column quarkColumn : quarkTile.cubeColumns) {
      posList.add(quarkColumn.cubeOrdinal);
    }

    for (Lattice.Measure measure : quarkTile.measures) {
      for (Lattice.Measure m : measures) {
        if (m.equals(measure)) {
          posList.add(((QuarkTile.Measure) measure).ordinal);
        }
      }
    }

    rel = RelOptUtil.createProject(rel, posList);

    int index = quarkTile.cubeColumns.size();
    for (AggregateCall aggCall : mergedAggregate.getAggCallList()) {
      final AggregateCall copy = AggregateCall.create(aggCall.getAggregation(),
          false, ImmutableList.of(index), -1, groupSet.cardinality(), rel, null, aggCall.name);
      aggCalls.add(copy);
      index++;
    }

    return mergedAggregate.copy(mergedAggregate.getTraitSet(), rel, false,
        groupSet.build(), null, aggCalls);
  }

  private Aggregate mergeAggregate(Aggregate aggregate1, Aggregate aggregate2) {
    //Support only simple groups
    if (aggregate1.getGroupType() != Aggregate.Group.SIMPLE
        || aggregate2.getGroupType() != Aggregate.Group.SIMPLE) {
      return null;
    }

    final int callLen1 = aggregate1.getAggCallList().size();
    final int callLen2 = aggregate2.getAggCallList().size();
    final List<AggregateCall> newAggCalls = Lists.newArrayList();
    if (callLen1 <= callLen2) {
      //Create new Call list
      for (AggregateCall call : aggregate1.getAggCallList()) {
        AggregateCall newAggCall = getMergedAggCall(aggregate2, call);
        if (newAggCall == null) {
          return null;
        } else {
          newAggCalls.add(newAggCall);
        }
      }

      //Create new groupSets
      ImmutableBitSet.Builder groupSetsBuilder = ImmutableBitSet.builder();
      for (int key : aggregate1.getGroupSet()) {
        try {
          groupSetsBuilder.set(aggregate2.getGroupSet().nth(key));
        } catch (IndexOutOfBoundsException e) {
          return null;
        }
      }
      final ImmutableBitSet newGroupSets = groupSetsBuilder.build();
      return aggregate1.copy(aggregate1.getTraitSet(), aggregate2.getInput(),
          aggregate1.indicator, newGroupSets, ImmutableList.of(newGroupSets), newAggCalls);
    } else {
      return null;
    }
  }

  private AggregateCall getMergedAggCall(Aggregate secondAgg, AggregateCall aggCall) {
    final int grouplen = secondAgg.getGroupSet().cardinality();
    final int callLen = secondAgg.getAggCallList().size();
    if (aggCall.getArgList().size() == 1) {
      final Integer arg = aggCall.getArgList().get(0);
      if (arg > (grouplen - 1) && arg < (grouplen + callLen)) {
        AggregateCall call2  = secondAgg.getAggCallList().get(arg - grouplen);
        if (call2.getAggregation() == aggCall.getAggregation()
            && call2.getArgList().size() == 1) {
          return call2.copy(call2.getArgList(), call2.filterArg);
        }
      }
    }
    return null;
  }

  String bitSetToString(ImmutableBitSet bits) {
    long result = 0;
    for (Integer i : bits) {
      result += LongMath.checkedPow(2, i);
    }

    return String.valueOf(result);
  }

  /**
   * Pushes a {@link org.apache.calcite.rel.core.Filter}
   * past a {@link org.apache.calcite.rel.core.Aggregate}.
   */
  RelNode filterAggregateTranspose(RelOptRuleCall call,
                                   Filter filterRel,
                                   Aggregate aggRel) {
    final List<RexNode> conditions =
        RelOptUtil.conjunctions(filterRel.getCondition());
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    final List<RelDataTypeField> origFields =
        aggRel.getRowType().getFieldList();
    final int[] adjustments = new int[origFields.size()];
    int i = 0;
    for (int key : aggRel.getGroupSet()) {
      adjustments[i] = key - i;
      i++;
    }
    final List<RexNode> pushedConditions = Lists.newArrayList();

    for (RexNode condition : conditions) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
      if (canPush(aggRel, rCols)) {
        pushedConditions.add(
            condition.accept(
                new RelOptUtil.RexInputConverter(rexBuilder, origFields,
                    aggRel.getInput(0).getRowType().getFieldList(),
                    adjustments)));
      } else {
        return null;
      }
    }

    final RelBuilder builder = call.builder();
    RelNode rel =
        builder.push(aggRel.getInput()).filter(pushedConditions).build();
    if (rel == aggRel.getInput(0)) {
      return null;
    }
    rel = aggRel.copy(aggRel.getTraitSet(), ImmutableList.of(rel));
    return rel;
  }

  /**
   * Checks if filter satisfies the lattice filter
   * i.e., it needs data captured by the lattice
   */
  private boolean isLatticeFilterSatisfied(final RelOptLattice lattice,
                                   final Filter filter,
                                   final StarTable.StarTableScan scan) {
    if (lattice.lattice.filter == null) {
      return true;
    }
    RexExecutorImpl rexImpl =
        (RexExecutorImpl) (scan.getCluster().getPlanner().getExecutor());
    RexImplicationChecker solver =
        new RexImplicationChecker(scan.getCluster().getRexBuilder(),
            rexImpl, scan.getRowType());
    try {
      return solver.implies(filter.getCondition(), lattice.lattice.filter);
    } catch (Exception e) {
      LOG.debug("Exception thrown while solving "
          + filter.getCondition()
          + "  =>  "
          + lattice.lattice.filter);
      return false;
    }
  }

  private boolean canPush(Aggregate aggregate, ImmutableBitSet rCols) {
    // If the filter references columns not in the group key, we cannot push
    final ImmutableBitSet groupKeys =
        ImmutableBitSet.range(0, aggregate.getGroupSet().cardinality());
    if (!groupKeys.contains(rCols)) {
      return false;
    }

    if (aggregate.indicator) {
      // If grouping sets are used, the filter can be pushed if
      // the columns referenced in the predicate are present in
      // all the grouping sets.
      for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
        if (!groupingSet.contains(rCols)) {
          return false;
        }
      }
    }
    return true;
  }
}
