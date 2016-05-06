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

import com.google.common.cache.Cache;

import com.qubole.quark.fatjdbc.QuarkConnectionImpl;
import com.qubole.quark.fatjdbc.QuarkJdbcStatement;
import com.qubole.quark.fatjdbc.QuarkMetaResultSet;
import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.parser.ParserResult;
import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.plugins.Executor;
import com.qubole.quark.plugins.jdbc.EMRDb;
import com.qubole.quark.plugins.jdbc.JdbcDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * Created by amoghm on 3/4/16.
 *
 * Parser for SQL statements that belongs to
 * {@link org.apache.calcite.sql.SqlKind#QUERY} or
 * {@link org.apache.calcite.sql.SqlKind#SET_QUERY}
 */
public class QueryPlanExecutor extends PlanExecutor {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryPlanExecutor.class);

  public static final int QUERY_TIMEOUT = 60;

  private final Meta.StatementHandle h;
  private final QuarkConnectionImpl connection;
  private final Cache<String, Connection> connectionCache;
  private final long maxRowCount;

  public QueryPlanExecutor(Meta.StatementHandle h, QuarkConnectionImpl connection,
                           Cache<String, Connection> connectionCache, long maxRowCount) {
    this.h = h;
    this.connection = connection;
    this.connectionCache = connectionCache;
    this.maxRowCount = maxRowCount;
  }

  public QuarkMetaResultSet execute(ParserResult result) throws Exception {
    QuarkMetaResultSet metaResultSet = null;
    String sql = result.getParsedSql();
    QuarkJdbcStatement stmt = connection.server.getStatement(h);
    final DataSourceSchema dataSourceSchema =
        ((SqlQueryParser.SqlQueryParserResult) result).getDataSource();
    if (dataSourceSchema != null) {
      Connection conn;
      final String id = dataSourceSchema.getName();
      Iterator<Object> iterator = null;
      Executor executor = (Executor) dataSourceSchema.getDataSource();
      String parsedSql = result.getParsedSql();
      LOG.info("Execute query[" + parsedSql + "]");
      if (executor instanceof JdbcDB) {
        conn = getExecutorConnection(id, executor);
        Statement statement = conn.createStatement();
        try {
          statement.setQueryTimeout(QUERY_TIMEOUT);
        } catch (Exception e) {
          LOG.warn("Couldnot set Query Timeout to " + QUERY_TIMEOUT + " seconds", e);
        }
        ResultSet resultSet = statement.executeQuery(parsedSql);
        metaResultSet = QuarkMetaResultSet.create(h.connectionId, h.id, resultSet,
            maxRowCount);
      } else {
        metaResultSet = getMetaResultSetFromIterator(executor.executeQuery(parsedSql),
            connection, result, sql, stmt, h, maxRowCount, null);
      }
    }
    return metaResultSet;
  }

  private Connection getExecutorConnection(String id, Executor executor)
      throws SQLException, ClassNotFoundException {
    Connection conn;
    if (executor instanceof EMRDb) {
      if (this.connectionCache.asMap().containsKey(id)) {
        conn = this.connectionCache.getIfPresent(id);
        if (conn.isClosed()) {
          conn = ((EMRDb) executor).getConnectionExec();
          this.connectionCache.put(id, conn);
        }
      } else {
        conn = ((EMRDb) executor).getConnectionExec();
        this.connectionCache.put(id, conn);
      }
    } else {
      if (this.connectionCache.asMap().containsKey(id)) {
        conn = this.connectionCache.getIfPresent(id);
        if (conn.isClosed()) {
          conn = ((JdbcDB) executor).getConnection();
          this.connectionCache.put(id, conn);
        }
      } else {
        conn = ((JdbcDB) executor).getConnection();
        this.connectionCache.put(id, conn);
      }
    }
    return conn;
  }
}
