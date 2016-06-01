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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlDropQuarkDataSource</code> is a node of a parse tree which represents a DROP DDL
 * statements for Quark DataSource.
 */
public class SqlDropQuarkDataSource extends SqlDropQuark {
  SqlSpecialOperator operator;

  public SqlDropQuarkDataSource(
      SqlParserPos pos,
      SqlIdentifier identifier) {
    super(pos, identifier);
    operator = new SqlSpecialOperator("DROP_DATASOURCE", SqlKind.OTHER_DDL);
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("DATASOURCE");
    identifier.unparse(writer, leftPrec, rightPrec);
  }
}

// End SqlDropQuarkDataSource.java
