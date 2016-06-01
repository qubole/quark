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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlCreateQuarkView</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public class SqlCreateQuarkView extends SqlCreateQuark {
  //~ Constructors -----------------------------------------------------------

  public SqlCreateQuarkView(SqlParserPos pos,
                                  SqlNode source,
                                  SqlNodeList columnList) {
    super(pos, source, columnList);
    operator = new SqlSpecialOperator("CREATE_VIEW", SqlKind.OTHER_DDL);
    operatorString = "CREATE VIEW";
  }
}

// End SqlCreateQuarkView.java
