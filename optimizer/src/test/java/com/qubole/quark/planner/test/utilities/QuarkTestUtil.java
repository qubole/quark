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

package com.qubole.quark.planner.test.utilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.Parser;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by amargoor on 10/29/15.
 */
public class QuarkTestUtil {
  /**
   * Hide the Constructor for utility class
   */
  private QuarkTestUtil() {

  }

  public static String toJson(String schema) throws JsonProcessingException {
    ImmutableList<String> defaultSchema = ImmutableList.of(schema);
    final ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(defaultSchema);
  }

  public static String getParsedSql(String sql, Properties info)
      throws QuarkException, SQLException {
    Parser parser = new Parser(info);
    return parser.parse(sql).getParsedSql();
  }

  public static void checkParsedSql(String sql, Properties info, String expectedSql)
      throws QuarkException, SQLException {
    String finalQuery = getParsedSql(sql, info);
    assertEquals(expectedSql, finalQuery);
  }

  public static void checkParsedSql(String sql, Parser parser, String expectedSql)
      throws QuarkException, SQLException {
    String finalQuery = parser.parse(sql).getParsedSql();
    assertEquals(expectedSql, finalQuery);
  }

  public static void checkSqlParsing(String sql, Properties info, String expectedSql,
      SqlDialect dialect)
      throws QuarkException, SqlParseException {
    Parser parser = new Parser(info);
    SqlParser sqlParser = parser.getSqlParser(sql);
    SqlNode sqlNode = sqlParser.parseQuery();
    String finalQuery = sqlNode.toSqlString(dialect).getSql();
    assertEquals(expectedSql, finalQuery.replace("\n", " "));
  }

  public static void checkParsedRelString(String sql,
                                          Properties info,
                                          ImmutableList<String> expected,
                                          ImmutableList<String> unexpected)
      throws QuarkException, SQLException {
    Parser parser = new Parser(info);
    RelNode relNode = parser.parse(sql).getRelNode();
    String relStr = RelOptUtil.toString(relNode);

    for (String expectedPlan : expected) {
      assertTrue("Final Plan should use table " + expectedPlan,
          relStr.contains(expectedPlan));
    }

    for (String unexpectedPlan : unexpected) {
      assertFalse("Final Plan should not use table " + unexpectedPlan,
          relStr.contains(unexpectedPlan));
    }
  }

  public static void checkParsedRelString(String sql,
                                          Parser parser,
                                          ImmutableList<String> expected,
                                          ImmutableList<String> unexpected)
      throws QuarkException, SQLException {
    RelNode relNode = parser.parse(sql).getRelNode();
    String relStr = RelOptUtil.toString(relNode);

    for (String expectedPlan : expected) {
      assertTrue("Final Plan should use table " + expectedPlan,
          relStr.contains(expectedPlan));
    }

    for (String unexpectedPlan : unexpected) {
      assertFalse("Final Plan should not use table " + unexpectedPlan,
          relStr.contains(unexpectedPlan));
    }
  }
}
