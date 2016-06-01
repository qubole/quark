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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Created by rajatv on 6/2/16.
 */
public abstract class SqlShowQuark extends SqlCall {
  SqlSpecialOperator operator;
  SqlNode likePattern;

  public SqlShowQuark(SqlParserPos pos, SqlNode likePattern) {
    super(pos);
    operator = new SqlSpecialOperator("SHOW_DATASOURCE", SqlKind.OTHER_DDL);
    this.likePattern = likePattern;
  }

  @Override public SqlKind getKind() {
    return SqlKind.OTHER_DDL;
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(likePattern);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
      case 0:
        likePattern = operand;
        break;
      default:
        throw new AssertionError(i);
    }
  }

  /**
   * Gets the filter condition for rows to be deleted.
   *
   * @return the condition expression for the data to be deleted, or null for
   * all rows in the table
   */
  public String getLikePattern() {
    return likePattern == null ? null : likePattern.toString().replaceAll("^'|'$", "");
  }

  @Override public abstract void unparse(SqlWriter writer, int leftPrec, int rightPrec);

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    throw new UnsupportedOperationException("Validation not supported for Quark's DDL");
  }
}
