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
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by adeshr on 5/3/16.
 */
public class SqlShowViews extends SqlShowQuark {
  SqlSpecialOperator operator;
  String operatorString;

  SqlNode likePattern;

  //~ Constructors -----------------------------------------------------------

  public SqlShowViews(
      SqlParserPos pos,
      SqlNode likePattern) {
    super(pos, likePattern);
    this.likePattern = likePattern;
    operator = new SqlSpecialOperator("SHOW_VIEW", SqlKind.OTHER_DDL);
    operatorString = "SHOW VIEW";
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("VIEW");
    if (likePattern != null) {
      writer.sep("LIKE");
      likePattern.unparse(writer, leftPrec, rightPrec);
    }
  }
}
