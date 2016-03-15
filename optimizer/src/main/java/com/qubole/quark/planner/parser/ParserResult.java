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
package com.qubole.quark.planner.parser;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

/**
 * Created by amoghm on 3/4/16.
 */
public abstract class ParserResult {
  protected final String parsedSql;
  protected SqlKind kind;

  protected final RelNode relNode;
  protected final boolean parseResult;
  public ParserResult(String parsedSql, SqlKind kind,
      RelNode relNode, boolean parseResult) {
    this.parsedSql = parsedSql;
    this.kind = kind;
    this.relNode = relNode;
    this.parseResult = parseResult;
  }

  public String getParsedSql() {
    return parsedSql;
  }

  public SqlKind getKind() {
    return kind;
  }

  public RelNode getRelNode() {
    return relNode;
  }

  public boolean isParseResult() {
    return parseResult;
  }
}
