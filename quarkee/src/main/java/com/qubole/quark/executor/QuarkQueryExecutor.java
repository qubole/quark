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
package com.qubole.quark.executor;

import com.google.common.cache.Cache;
import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.parser.ParserResult;
import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.plugins.Executor;
import com.qubole.quark.plugins.jdbc.EMRDb;
import com.qubole.quark.plugins.jdbc.JdbcDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by adeshr on 5/24/16.
 */
public class QuarkQueryExecutor implements QuarkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(QuarkQueryExecutor.class);

  public static final int QUERY_TIMEOUT = 60;

  private final Cache<String, Connection> connectionCache;

  public QuarkQueryExecutor(Cache<String, Connection> connectionCache) {
    this.connectionCache = connectionCache;
  }

  public Object execute(ParserResult parserResult) throws Exception {

    final DataSourceSchema dataSourceSchema =
        ((SqlQueryParser.SqlQueryParserResult) parserResult).getDataSource();

    if (dataSourceSchema != null) {
      Connection conn;
      final String id = dataSourceSchema.getName();
      Executor executor = (Executor) dataSourceSchema.getDataSource();
      String parsedSql = parserResult.getParsedSql();

      LOG.info("Execute query[" + parsedSql + "]");

      if (executor instanceof JdbcDB) {
        conn = getExecutorConnection(id, executor);
        Statement statement = conn.createStatement();
        try {
          statement.setQueryTimeout(QUERY_TIMEOUT);
        } catch (Exception e) {
          LOG.warn("Couldnot set Query Timeout to " + QUERY_TIMEOUT + " seconds", e);
        }
        return statement.executeQuery(parsedSql);
      } else {
        return executor.executeQuery(parsedSql);
      }
    }
    return null;
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
