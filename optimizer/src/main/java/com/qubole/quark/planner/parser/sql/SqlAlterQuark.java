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
  SqlIdentifier identifier;

  //~ Constructors -----------------------------------------------------------

  public SqlAlterQuark(SqlParserPos pos,
                       SqlNodeList targetColumnList,
                       SqlNodeList sourceExpressionList,
                       SqlIdentifier identifier) {
    super(pos);
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
    this.identifier = identifier;
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
        sourceExpressionList, identifier);
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
        identifier = (SqlIdentifier) operand;
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
  public SqlIdentifier getIdentifier() {
    return identifier;
  }

  @Override public abstract void unparse(SqlWriter writer, int leftPrec, int rightPrec);

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    throw new UnsupportedOperationException("No validation supported for"
        + " Quark's DDL statements");
  }
}

// End SqlAlterQuark.java
