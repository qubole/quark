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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/**
 * A <code>SqlAlterQuarkView</code> is a node of a parse tree which represents an ALTER
 * metadata for Quark View
 */
public class SqlAlterQuarkView extends SqlAlterQuark {
  //~ Constructors -----------------------------------------------------------

  public SqlAlterQuarkView(SqlParserPos pos,
                       SqlNodeList targetColumnList,
                       SqlNodeList sourceExpressionList,
                       SqlIdentifier identifier) {
    super(pos, targetColumnList, sourceExpressionList, identifier);
    operator = new SqlSpecialOperator("ALTER_VIEW", SqlKind.OTHER_DDL);
    operatorString = "ALTER VIEW";
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("VIEW");
    identifier.unparse(writer, leftPrec, rightPrec);
    writer.keyword("SET");
    for (Pair<SqlNode, SqlNode> pair
        : Pair.zip(getTargetColumnList(), getSourceExpressionList())) {
      writer.sep(",");
      SqlIdentifier id = (SqlIdentifier) pair.left;
      id.unparse(writer, leftPrec, rightPrec);
      writer.keyword("=");
      SqlNode sourceExp = pair.right;
      sourceExp.unparse(writer, leftPrec, rightPrec);
    }
  }

}

// End SqlAlterQuarkView.java
