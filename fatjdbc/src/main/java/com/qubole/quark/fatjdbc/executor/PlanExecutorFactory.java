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
package com.qubole.quark.fatjdbc.executor;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.sql.SqlKind;

import com.google.common.cache.Cache;

import com.qubole.quark.fatjdbc.QuarkConnectionImpl;

import java.sql.Connection;

/**
 * Created by amoghm on 3/4/16.
 */
public class PlanExecutorFactory {
  private PlanExecutorFactory() {
  }

  public static PlanExecutor buildPlanExecutor(SqlKind kind, Meta.StatementHandle h,
      QuarkConnectionImpl connection, Cache<String, Connection> connectionCache,
      long maxRowCount) {
    if (kind.equals(SqlKind.OTHER_DDL)) {
      return new DDLPlanExecutor(h, connection);
    } else if (kind.belongsTo(SqlKind.QUERY)) {
      return new QueryPlanExecutor(h, connection, connectionCache, maxRowCount);
    } else {
      throw new UnsupportedOperationException("Cannot execute parsed Sql of kind: "
          + kind);
    }
  }
}
