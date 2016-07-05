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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * A <code>SqlCreateQuarkView</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public class SqlCreateQuarkView extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_VIEW", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos,
                              SqlNode... operands) {
      return new SqlCreateQuarkView(pos, (SqlIdentifier) operands[0],
          (SqlIdentifier) operands[1], operands[2]);
    }
  };

  private SqlIdentifier viewName;
  private SqlIdentifier tableName;
  private SqlNode query;

  public SqlCreateQuarkView(SqlParserPos pos, SqlIdentifier viewName,
                            SqlIdentifier tableName, SqlNode query) {
    super(pos);
    this.viewName = viewName;
    this.query = query;
    this.tableName = tableName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public SqlKind getKind() {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(viewName);
    ops.add(tableName);
    ops.add(query);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("VIEW");
    viewName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("STORED");
    writer.keyword("IN");
    tableName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  public List<String> getSchemaPath() {
    if (viewName.isSimple()) {
      return ImmutableList.of();
    }

    return viewName.names.subList(0, viewName.names.size() - 1);
  }

  public String getName() {
    if (viewName.isSimple()) {
      return viewName.getSimple();
    }

    return viewName.names.get(viewName.names.size() - 1);
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }

  public SqlNode getQuery() {
    return query;
  }
}

// End SqlCreateQuarkView.java
