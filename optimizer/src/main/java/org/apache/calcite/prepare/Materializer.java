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

package org.apache.calcite.prepare;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.qubole.quark.planner.QuarkTableScan;
import com.qubole.quark.planner.QuarkTileScan;
import com.qubole.quark.planner.QuarkViewScan;
import com.qubole.quark.planner.QuarkViewTable;

import javax.annotation.Nullable;

/**
 * Created by amoghm on 11/19/15.
 *
 * 1. Populates a materialization record, converting a table path
 * (essentially a list of strings, like ["hr", "sales"]) into a table object
 * that can be used in the planning process.
 *
 * 2. Adds to the planner
 */
public class Materializer {
  MaterializePrepare prepare = new MaterializePrepare();
  public void populateMaterializations(CalcitePrepare.Context context,
      RelOptPlanner planner, Prepare.Materialization materialization) {
    prepare.populateMaterializations(context, planner, materialization);

  }

  /**
   * Inherited from {@link CalcitePrepareImpl} to
   * make populateMaterializations accessible
   */
  private class MaterializePrepare extends CalcitePrepareImpl {
    public void populateMaterializations(Context context,
                                  RelOptPlanner planner,
                                  Prepare.Materialization materialization) {
      try {
        final CalciteSchema schema = materialization.materializedTable.schema;
        CalciteCatalogReader catalogReader =
            new CalciteCatalogReader(
                schema.root(),
                context.config().caseSensitive(),
                Util.skipLast(materialization.materializedTable.path()),
                context.getTypeFactory());
        final QuarkMaterializer materializer =
            new QuarkMaterializer(this, context, catalogReader, schema, planner);
        materializer.populate(materialization);
      } catch (Exception e) {
        throw new RuntimeException("While populating materialization "
            + materialization.materializedTable.path(), e);
      }
      planner.addMaterialization(
          new RelOptMaterialization(materialization.tableRel,
              materialization.queryRel,
              materialization.getStarTableIdentified()));
    }
  }

  /**
   * Context for populating a {@link Materialization}.
   */
  private class QuarkMaterializer extends CalciteMaterializer {

    QuarkMaterializer(CalcitePrepareImpl prepare,
                             CalcitePrepare.Context context,
                             CatalogReader catalogReader,
                             CalciteSchema schema,
                             RelOptPlanner planner) {
      super(prepare, context, catalogReader, schema, planner);
    }
    /** Populates a materialization record, converting a table path
     * (essentially a list of strings, like ["hr", "sales"]) into a table object
     * that can be used in the planning process. */
    void populate(Materialization materialization) {

      if (materialization.queryRel != null && materialization.tableRel != null) {
        final RelOptCluster oldCluster = materialization.queryRel.getCluster();
        RelOptCluster cluster = RelOptCluster.create(planner, oldCluster.getRexBuilder());
        materialization.queryRel = materialization.queryRel.accept(new ClusterShuttle(cluster));
        materialization.tableRel = materialization.tableRel.accept(new ClusterShuttle(cluster));
      } else {
        super.populate(materialization);
      }
    }
  }

  /**
   * Creates copy of {@link RelNode} with new {@link org.apache.calcite.plan.RelOptCluster}
   */
  static class ClusterShuttle implements RelShuttle {
    private RelOptCluster cluster;
    private Function<RelNode, RelNode> transform = new Function<RelNode, RelNode>() {
      @Nullable
      @Override
      public RelNode apply(RelNode relNode) {
        return relNode.accept(new ClusterShuttle(cluster));
      }
    };
    ClusterShuttle(RelOptCluster cluster) {
      this.cluster = cluster;
    }
    public RelNode visit(TableScan scan) {
      final RelNode transformed;
      if (scan instanceof QuarkTileScan) {
        transformed = new QuarkTileScan(cluster, scan.getTable(),
            ((QuarkTileScan) scan).getQuarkTile(),
            ((QuarkTileScan) scan).getQuarkTable());
      } else if (scan instanceof QuarkViewScan) {
        transformed = new QuarkViewScan(cluster, scan.getTable(),
            (QuarkViewTable) ((QuarkViewScan) scan).getQuarkTable());
      } else if (scan instanceof QuarkTableScan) {
        transformed = new QuarkTableScan(cluster, scan.getTable(),
            ((QuarkTableScan) scan).getQuarkTable());
      } else if (scan instanceof LogicalTableScan) {
        transformed = new LogicalTableScan(cluster, scan.getTraitSet(), scan.getTable());
      } else if (scan instanceof StarTable.StarTableScan) {
        transformed = new StarTable.StarTableScan(cluster, scan.getTable());
      } else {
        throw new UnsupportedOperationException(scan.getClass().getName()
            + " is not yet supported during materialization");
      }
      transformed.register(cluster.getPlanner());
      return transformed;
    }
    public RelNode visit(TableFunctionScan scan) {
      scan.register(cluster.getPlanner());
      if (scan instanceof LogicalTableFunctionScan) {
        return new LogicalTableFunctionScan(
            cluster, scan.getTraitSet(),
            Lists.transform(scan.getInputs(), transform),
            scan.getCall(), scan.getElementType(),
            scan.getRowType(), scan.getColumnMappings());
      }
      throw new UnsupportedOperationException(scan.getClass().getName()
          + " is not yet supported during materialization");
    }
    public RelNode visit(LogicalValues values) {
      return new LogicalValues(cluster, values.getTraitSet(),
          values.getRowType(), values.getTuples());
    }
    public RelNode visit(LogicalFilter filter) {
      return new LogicalFilter(cluster, filter.getTraitSet(),
          filter.getInput().accept(this), filter.getCondition());
    }
    public RelNode visit(LogicalProject project) {
      return new LogicalProject(cluster, project.getTraitSet(),
          project.getInput().accept(this), project.getProjects(),
          project.getRowType());
    }
    public RelNode visit(LogicalJoin join) {
      return new LogicalJoin(
          cluster,
          join.getTraitSet(),
          join.getLeft().accept(this),
          join.getRight().accept(this),
          join.getCondition(),
          join.getJoinType(),
          join.getVariablesStopped(),
          join.isSemiJoinDone(),
          ImmutableList.copyOf(join.getSystemFieldList()));
    }
    public RelNode visit(LogicalCorrelate correlate) {
      return new LogicalCorrelate(cluster, correlate.getTraitSet(),
          correlate.getLeft().accept(this), correlate.getRight().accept(this),
          correlate.getCorrelationId(), correlate.getRequiredColumns(),
          correlate.getJoinType());
    }
    public RelNode visit(LogicalUnion union) {
      return new LogicalUnion(cluster, union.getTraitSet(),
          Lists.transform(union.getInputs(), transform),
          union.all);
    }
    public RelNode visit(LogicalIntersect intersect) {
      return new LogicalIntersect(cluster, intersect.getTraitSet(),
      Lists.transform(intersect.getInputs(), transform), intersect.all);
    }
    public RelNode visit(LogicalMinus minus) {
      return new LogicalMinus(cluster, minus.getTraitSet(),
          Lists.transform(minus.getInputs(), transform), minus.all);
    }
    public RelNode visit(LogicalAggregate aggregate) {
      return new LogicalAggregate(cluster,
          aggregate.getTraitSet(),
          aggregate.getInput().accept(this),
          aggregate.indicator,
          aggregate.getGroupSet(),
          aggregate.getGroupSets(),
          aggregate.getAggCallList());
    }
    public RelNode visit(LogicalSort sort) {
      return LogicalSort.create(sort.getInput().accept(this),
          sort.getCollation(), sort.offset, sort.fetch);
    }
    public RelNode visit(LogicalExchange exchange) {
      return LogicalExchange.create(exchange.getInput().accept(this),
          exchange.getDistribution());
    }
    public RelNode visit(RelNode other) {
      throw new UnsupportedOperationException(other.getClass().getName()
          + " is not supported during materialization");
    }
  }
}
