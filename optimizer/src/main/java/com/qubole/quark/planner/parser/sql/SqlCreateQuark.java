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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public abstract class SqlCreateQuark extends SqlCall {
  protected SqlSpecialOperator operator;
  protected String operatorString;

  SqlNode source;
  SqlNodeList columnList;

  //~ Constructors -----------------------------------------------------------

  public SqlCreateQuark(SqlParserPos pos,
                        SqlNode source,
                        SqlNodeList columnList) {
    super(pos);
    this.source = source;
    this.columnList = columnList;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.OTHER_DDL;
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(source, columnList);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      source = operand;
      break;
    case 1:
      columnList = (SqlNodeList) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the source expression for the data to be inserted
   */
  public SqlNode getSource() {
    return source;
  }

  public void setSource(SqlSelect source) {
    this.source = source;
  }

  /**
   * @return the list of target column names, or null for all columns in the
   * target table
   */
  public SqlNodeList getTargetColumnList() {
    return columnList;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep(operatorString);
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    if (columnList != null) {
      columnList.unparse(writer, opLeft, opRight);
    }
    writer.newlineAndIndent();
    source.unparse(writer, opLeft, opRight);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    throw new UnsupportedOperationException("Validation not supported"
        + " currently for Quark DDL");
  }
}

// End SqlCreateQuark.java
