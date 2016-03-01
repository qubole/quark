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

package com.qubole.quark.planner.test;

import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.planner.TestFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.QuarkColumn;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Created by rajatv on 2/6/15.
 */
public class QueryTest {

  private static final Logger log = LoggerFactory.getLogger(QueryTest.class);
  private static Properties info;

  protected static class QueryTestSchema extends TestSchema {
    public QueryTestSchema(String name) {
      super(name);
    }

    @Override
    protected Map<String, Table> getTableMap() {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      QuarkTable simple = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("I", Types.INTEGER));
      }});
      builder.put("SIMPLE", simple);

      QuarkTable many_columns = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("I", Types.INTEGER));
        add(new QuarkColumn("J", Types.INTEGER));
        add(new QuarkColumn("K", Types.VARCHAR));
      }});
      builder.put("MANY_COLUMNS", many_columns);

      return builder.build();
    }
  }

  public static class SchemaFactory implements TestFactory {
    @Override
    public List<QuarkSchema> create(Properties info) throws QuarkException {
      try {
        return Collections.singletonList((QuarkSchema) new QueryTestSchema("TEST"));
      } catch (Exception c) {
        log.error(c.getMessage());
        return null;
      }
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    info = new Properties();
    info.put("unitTestMode", "true");
    info.put("defaultSchema", QuarkTestUtil.toJson("TEST"));
    info.put("schemaFactory", "com.qubole.quark.planner.test.QueryTest$SchemaFactory");
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'TEST',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'TEST',\n"
            + "       factory: 'com.qubole.quark.planner.test.QueryTest$SchemaFactory',\n"
            + "       operand: {}\n"
            + "     }\n"
            + "   ]\n"
            + "}");
  }

  @Test
  public void testParse() throws QuarkException, SQLException {
    SqlQueryParser parser = new SqlQueryParser(info);
    RelNode relNode = parser.parse("select * from simple").getRelNode();
    List<String> usedTables = parser.getTables(relNode);

    assertThat(usedTables).contains("TEST.SIMPLE");
  }

  @Test
  public void testFilter() throws QuarkException, SQLException {
    SqlQueryParser parser = new SqlQueryParser(info);

    SqlQueryParser.SqlQueryParserResult result =
        parser.parse("select count(*) from test.many_columns where many_columns.j > 100");
    List<String> usedTables = parser.getTables(result.getRelNode());
    assertThat(usedTables).contains("TEST.MANY_COLUMNS");

    assertThat(
        result.getParsedSql()
            .equals("SELECT COUNT(*) FROM TEST.MANY_COLUMNS WHERE J > 100")
    );
  }

  @Test
  public void testTwoFilter() throws QuarkException, SQLException {
    SqlQueryParser parser = new SqlQueryParser(info);

    SqlQueryParser.SqlQueryParserResult result =
        parser.parse("select count(*) from test.many_columns where " +
            "test.many_columns.j > 100 and test.many_columns.i = 10");


    List<String> usedTables = parser.getTables(result.getRelNode());
    assertThat(usedTables).contains("TEST.MANY_COLUMNS");

    assertThat(
        result.getParsedSql()
            .equals("SELECT COUNT(*) FROM TEST.MANY_COLUMNS WHERE J > 100 AND I = 10")
    );
  }

  @Test
  public void testSyntaxError() throws QuarkException, SQLException {
    SqlQueryParser parser = new SqlQueryParser(info);
    try {
      parser.parse("select count(*) test.many_columns where " +
          "test.many_columns.j > 100 and test.many_columns.i = 10");
      failBecauseExceptionWasNotThrown(SQLException.class);
    } catch (SQLException e) {
      assertThat((Throwable) e).hasMessageContaining("Encountered \".\" at line 1, column 21.");
    }
  }

  @Test
  public void testSemanticError() throws QuarkException, SQLException {
    SqlQueryParser parser = new SqlQueryParser(info);
    try {
      parser.parse("select count(*) from test.many_colum where " +
          "test.many_columns.j > 100 and test.many_columns.i = 10");
      failBecauseExceptionWasNotThrown(SQLException.class);
    } catch (SQLException e) {
      assertThat((Throwable) e).hasMessageContaining("Table 'TEST.MANY_COLUM' not found");
    }
  }
}
