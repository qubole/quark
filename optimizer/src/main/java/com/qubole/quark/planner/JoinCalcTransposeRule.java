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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import java.util.List;

/**
 * Created by amoghm on 4/4/16.
 */
public class JoinCalcTransposeRule extends RelOptRule {
  public static final JoinCalcTransposeRule LEFT_CALC =
      new JoinCalcTransposeRule(
          operand(Join.class,
              operand(Calc.class, any()),
              operand(RelNode.class, any())),
          "JoinCalcTransposeRule(Project-Other)");

  public static final JoinCalcTransposeRule RIGHT_CALC =
      new JoinCalcTransposeRule(
          operand(
              Join.class,
              operand(RelNode.class, any()),
              operand(Calc.class, any())),
          "JoinCalcTransposeRule(Other-Project)");

  public JoinCalcTransposeRule(
      RelOptRuleOperand operand,
      String description) {
    super(operand, RelFactories.LOGICAL_BUILDER, description);
  }

  public void onMatch(RelOptRuleCall call) {
    final Join joinRel = call.rel(0);
    final RelNode otherNode;
    final Calc calc;

    final RelNode leftJoinChild;
    final RelNode rightJoinChild;

    if (call.rel(1) instanceof Calc) {
      otherNode = call.rel(2);
      calc = call.rel(1);
      rightJoinChild = otherNode;
      leftJoinChild = calc.getInput();
    } else {
      otherNode = call.rel(1);
      calc = call.rel(2);
      rightJoinChild = calc.getInput();
      leftJoinChild = otherNode;
    }
    /**
     * Currently not supporting calc which doesnot
     * project star (all the columns of input)
     * or has aggregates.
     */
    if (!isStar(calc.getProgram())
        || calc.getProgram().containsAggs()) {
      return;
    }

    final List<RelDataTypeField> origFields =
        calc.getRowType().getFieldList();
    final int[] adjustments = new int[calc.getProgram().getExprCount()];
    if (rightJoinChild == calc.getInput()) {
      int offset = leftJoinChild.getRowType().getFieldList().size();
      for (int i = 0; i < origFields.size(); i++) {
        adjustments[i] = offset;
      }
    }
    Join newJoinRel =
        joinRel.copy(joinRel.getTraitSet(), joinRel.getCondition(),
            leftJoinChild, rightJoinChild, joinRel.getJoinType(),
            joinRel.isSemiJoinDone());

    RexProgramBuilder topProgramBuilder =
        new RexProgramBuilder(
            joinRel.getRowType(),
            joinRel.getCluster().getRexBuilder());
    topProgramBuilder.addIdentity();
    final RelOptUtil.RexInputConverter rexInputConverter =
        new RelOptUtil.RexInputConverter(calc.getCluster().getRexBuilder(),
            origFields,
            joinRel.getRowType().getFieldList(),
            adjustments);
    if (calc.getProgram().getCondition() != null) {
      RexNode cond =
          calc.getProgram().expandLocalRef(calc.getProgram().getCondition());
      final RexLocalRef rexLocalRef =
          topProgramBuilder.addExpr(cond.accept(rexInputConverter));
      topProgramBuilder.addCondition(rexLocalRef);
    }
    Calc newCalcRel =
        calc.copy(calc.getTraitSet(), newJoinRel, topProgramBuilder.getProgram());

    call.transformTo(newCalcRel);
  }

  private static boolean isStar(RexProgram program) {
    int i = 0;

    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() != i++) {
        return false;
      }
    }
    return i == program.getInputRowType().getFieldCount();
  }
}
