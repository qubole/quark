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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import com.qubole.quark.planner.parser.SqlQueryParser;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cluster that allows change of {@link RelOptPlanner}.
 *
 * This Cluster is used to optimize populating Materialized views in {@link RelOptPlanner}
 * and should be used only in {@link RelNode} of Materialized views.
 * Populating materialized views requires parsing of sql string representing
 * materialized views and this can be very expensive. To optimize, for a
 * {@link SqlQueryParser} object all view Sql strings are
 * parsed only once and cached. However, caching doesn't work as every new parse
 * creates new {@link RelOptPlanner} which does not accept cached {@link RelNode}
 * created using dofferent planner.
 * If cached {@link RelNode} consists of QuarkMaterializedCluster then
 * we change the planner to the new planner and then it can accept cached
 * {@link RelNode}.
 */
public class QuarkMaterializeCluster extends RelOptCluster {
  final RelOptPlannerHolder plannerHolder;

  /**
   * Creates a cluster.
   *
   * <p>For use only from {@link #create}.
   */
  QuarkMaterializeCluster(RelOptPlannerHolder plannerHolder,
                          RelDataTypeFactory typeFactory,
                          RexBuilder rexBuilder, AtomicInteger nextCorrel,
                          Map<String, RelNode> mapCorrelToRel) {
    super(plannerHolder.getPlanner(), typeFactory, rexBuilder, nextCorrel, mapCorrelToRel);
    this.plannerHolder = plannerHolder;
  }
  /** Creates a cluster. */
  public static QuarkMaterializeCluster create(RelOptPlannerHolder plannerHolder,
                                     RexBuilder rexBuilder) {
    return new QuarkMaterializeCluster(plannerHolder, rexBuilder.getTypeFactory(),
        rexBuilder, new AtomicInteger(0), new HashMap<String, RelNode>());
  }
  public RelOptPlanner getPlanner() {
    return plannerHolder.getPlanner();
  }
  /** Returns the default trait set for this cluster. */
  public RelTraitSet traitSet() {
    return getPlanner().emptyTraitSet();
  }

  public RelTraitSet traitSetOf(RelTrait trait) {
    return traitSet().replace(trait);
  }

  /**
   * Holder for {@link RelOptPlanner}
   */
  public static class RelOptPlannerHolder {
    private RelOptPlanner planner;

    public RelOptPlannerHolder(RelOptPlanner planner) {
      this.planner = planner;
    }

    public RelOptPlanner getPlanner() {
      return planner;
    }

    public void setPlanner(RelOptPlanner planner) {
      this.planner = planner;
    }
  }
}


