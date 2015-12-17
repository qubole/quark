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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Planner rule that projects from a {@link QuarkTableScan} scan just the columns
 * needed to satisfy a projection. If the projection's expressions are trivial,
 * the projection is removed.
 */
public class ProjectRule extends RelOptRule {
  public static final ProjectRule INSTANCE =
      new ProjectRule();

  private ProjectRule() {
    super(
        operand(LogicalProject.class,
            operand(QuarkTableScan.class, none())),
        "ProjectRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final QuarkTableScan scan = call.rel(1);
    if (!complexExpressions(project.getProjects())) {
      call.transformTo(
          new QuarkTableScan(
              scan.getCluster(),
              scan.getTable(),
              scan.quarkTable));
    }
  }

  private boolean complexExpressions(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (!(exp instanceof RexInputRef
          || exp instanceof RexLiteral)) { //RexLiteral surfaces for aggregate count(*)
        return false; // not a simple projection
      }
    }
    return true;
  }
}
