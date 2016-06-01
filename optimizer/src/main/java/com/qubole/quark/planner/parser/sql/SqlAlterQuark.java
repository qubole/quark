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
package com.qubole.quark.planner.parser.sql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * A <code>SqlAlterQuark</code> is a node of a parse tree which represents an ALTER
 * metadata for Quark
 */
public abstract class SqlAlterQuark extends SqlCall {
  protected SqlSpecialOperator operator;
  protected String operatorString;

  SqlNodeList targetColumnList;
  SqlNodeList sourceExpressionList;
  SqlNode condition;

  //~ Constructors -----------------------------------------------------------

  public SqlAlterQuark(SqlParserPos pos,
                       SqlNodeList targetColumnList,
                       SqlNodeList sourceExpressionList,
                       SqlNode condition) {
    super(pos);
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
    this.condition = condition;
    assert sourceExpressionList.size() == targetColumnList.size();
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.OTHER_DDL;
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetColumnList,
        sourceExpressionList, condition);
  }

  @Override public void setOperand(int i, SqlNode operand) {

    switch (i) {
      case 0:
        targetColumnList = (SqlNodeList) operand;
        break;
      case 1:
        sourceExpressionList = (SqlNodeList) operand;
        break;
      case 2:
        condition = operand;
        break;
      default:
        throw new AssertionError(i);
    }
  }

  /**
   * @return the list of target column names
   */
  public SqlNodeList getTargetColumnList() {
    return targetColumnList;
  }

  /**
   * @return the list of source expressions
   */
  public SqlNodeList getSourceExpressionList() {
    return sourceExpressionList;
  }

  /**
   * Gets the filter condition for rows to be updated.
   *
   * @return the condition expression for the data to be updated, or null for
   * all rows in the table
   */
  public SqlNode getCondition() {
    return condition;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, operatorString, "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    final SqlWriter.Frame setFrame =
        writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, "SET", "");
    for (Pair<SqlNode, SqlNode> pair
        : Pair.zip(getTargetColumnList(), getSourceExpressionList())) {
      writer.sep(",");
      SqlIdentifier id = (SqlIdentifier) pair.left;
      id.unparse(writer, opLeft, opRight);
      writer.keyword("=");
      SqlNode sourceExp = pair.right;
      sourceExp.unparse(writer, opLeft, opRight);
    }
    writer.endList(setFrame);
    if (condition != null) {
      writer.sep("WHERE");
      condition.unparse(writer, opLeft, opRight);
    }
    writer.endList(frame);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    throw new UnsupportedOperationException("No validation supported for"
        + " Quark's DDL statements");
  }
}

// End SqlAlterQuark.java
