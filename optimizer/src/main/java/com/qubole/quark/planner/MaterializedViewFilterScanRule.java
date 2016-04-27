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

import org.apache.calcite.plan.MaterializedViewSubstitutionVisitor;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;

import java.util.Collections;
import java.util.List;

/**
 * Planner rule that converts
 * a {@link org.apache.calcite.rel.core.Filter}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * to a filter on Materialized View
 */
public abstract class MaterializedViewFilterScanRule extends RelOptRule {
  /** Rule that matches Filter on TableScan. */
  public static final MaterializedViewFilterScanRule INSTANCE =
      new MaterializedViewFilterScanRule(
          operand(Filter.class,
              operand(TableScan.class, null, none())),
          "MaterializedViewFilterTableRule") {
        public void onMatch(RelOptRuleCall call) {
          final Filter filter = call.rel(0);
          final TableScan scan = call.rel(1);
          apply(call, filter, scan);
        }
      };
  private final HepProgram program = new HepProgramBuilder()
      .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
      .addRuleInstance(ProjectMergeRule.INSTANCE)
      .build();

  //~ Constructors -----------------------------------------------------------

  /** Creates a FilterTableRule. */
  protected MaterializedViewFilterScanRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  //~ Methods ----------------------------------------------------------------

  protected void apply(RelOptRuleCall call, Filter filter, TableScan scan) {
    //Avoid optimizing already optimized scan
    if (scan instanceof QuarkViewScan || scan instanceof QuarkTileScan) {
      return;
    }
    RelNode root = filter.copy(filter.getTraitSet(),
        Collections.singletonList((RelNode) scan));
    RelOptPlanner planner = call.getPlanner();
    if (planner instanceof VolcanoPlanner) {
      List<RelOptMaterialization> materializations
          = ((VolcanoPlanner) planner).getApplicableMaterializations();
      for (RelOptMaterialization materialization : materializations) {
        if (scan.getRowType().equals(materialization.queryRel.getRowType())) {
          RelNode target = materialization.queryRel;
          final HepPlanner hepPlanner =
              new HepPlanner(program, planner.getContext());
          hepPlanner.setRoot(target);
          target = hepPlanner.findBestExp();
          List<RelNode> subs = new MaterializedViewSubstitutionVisitor(target, root)
              .go(materialization.tableRel);
          for (RelNode s : subs) {
            call.transformTo(s);
          }
        }
      }
    }
  }
}

// End MaterializedViewFilterScanRule.java
