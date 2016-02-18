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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.*;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;
import com.qubole.quark.sql.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 2/6/15.
 */
public class SchemaTest {
  private static final Logger log = LoggerFactory.getLogger(QueryTest.class);
  private static Properties info;

  public static class DefaultSchema extends TestSchema {
    DefaultSchema(String name) {
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

  public static class TpchSchema extends TestSchema {

    public static String part_comp1_rel = "QuarkViewScan(table=[[TPCH, PART_COMP1]], " +
        "fields=[RecordType(JavaType(class java.lang.Integer) P_PARTKEY, JavaType(class java.lang.String) P_NAME, " +
        "JavaType(class java.lang.String) P_MFGR, JavaType(class java.lang.String) P_BRAND, " +
        "JavaType(class java.lang.String) P_TYPE, JavaType(class java.lang.Integer) P_SIZE, " +
        "JavaType(class java.lang.String) P_CONTAINER, JavaType(class java.lang.Double) P_RETAILPRICE, " +
        "JavaType(class java.lang.String) P_COMMENT)])";

    public static String part_rel = "QuarkTableScan(table=[[TPCH, PART]], " +
        "fields=[RecordType(JavaType(class java.lang.Integer) P_PARTKEY, JavaType(class java.lang.String) P_NAME, " +
        "JavaType(class java.lang.String) P_MFGR, JavaType(class java.lang.String) P_BRAND, " +
        "JavaType(class java.lang.String) P_TYPE, JavaType(class java.lang.Integer) P_SIZE, " +
        "JavaType(class java.lang.String) P_CONTAINER, JavaType(class java.lang.Double) P_RETAILPRICE, " +
        "JavaType(class java.lang.String) P_COMMENT)])\n";

    TpchSchema(String name) {
      super(name);
    }

    @Override
    protected Map<String, Table> getTableMap() {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      QuarkTable simple = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("N_NATIONKEY", Types.INTEGER));
        add(new QuarkColumn("N_NAME", Types.VARCHAR));
        add(new QuarkColumn("N_REGIONKEY", Types.INTEGER));
        add(new QuarkColumn("N_COMMENT", Types.VARCHAR));
      }});
      builder.put("NATION", simple);

      QuarkTable many_columns = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("R_REGIONKEY", Types.INTEGER));
        add(new QuarkColumn("R_NAME", Types.VARCHAR));
        add(new QuarkColumn("R_COMMENT", Types.VARCHAR));
      }});
      builder.put("REGION", many_columns);

      QuarkTable part = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("P_PARTKEY", Types.INTEGER));
        add(new QuarkColumn("P_NAME", Types.VARCHAR));
        add(new QuarkColumn("P_MFGR", Types.VARCHAR));
        add(new QuarkColumn("P_BRAND", Types.VARCHAR));
        add(new QuarkColumn("P_TYPE", Types.VARCHAR));
        add(new QuarkColumn("P_SIZE", Types.INTEGER));
        add(new QuarkColumn("P_CONTAINER", Types.VARCHAR));
        add(new QuarkColumn("P_RETAILPRICE", Types.DOUBLE));
        add(new QuarkColumn("P_COMMENT", Types.VARCHAR));
      }});
      builder.put("PART", part);

      QuarkTable part_comp1 = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("P_PARTKEY", Types.INTEGER));
        add(new QuarkColumn("P_NAME", Types.VARCHAR));
        add(new QuarkColumn("P_MFGR", Types.VARCHAR));
        add(new QuarkColumn("P_BRAND", Types.VARCHAR));
        add(new QuarkColumn("P_TYPE", Types.VARCHAR));
        add(new QuarkColumn("P_SIZE", Types.INTEGER));
        add(new QuarkColumn("P_CONTAINER", Types.VARCHAR));
        add(new QuarkColumn("P_RETAILPRICE", Types.DOUBLE));
        add(new QuarkColumn("P_COMMENT", Types.VARCHAR));
      }});
      builder.put("PART_COMP1", part_comp1);

      QuarkTable part_100 = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("P_PARTKEY", Types.INTEGER));
        add(new QuarkColumn("P_NAME", Types.VARCHAR));
        add(new QuarkColumn("P_MFGR", Types.VARCHAR));
        add(new QuarkColumn("P_BRAND", Types.VARCHAR));
        add(new QuarkColumn("P_TYPE", Types.VARCHAR));
        add(new QuarkColumn("P_SIZE", Types.INTEGER));
        add(new QuarkColumn("P_CONTAINER", Types.VARCHAR));
        add(new QuarkColumn("P_RETAILPRICE", Types.DOUBLE));
        add(new QuarkColumn("P_COMMENT", Types.VARCHAR));
      }});
      builder.put("PART_100", part_100);

      QuarkTable sales = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("P_SALESID", Types.INTEGER));
        add(new QuarkColumn("P_PRODUCTKEY", Types.INTEGER));
        add(new QuarkColumn("P_SALEDATE", Types.DATE));
        add(new QuarkColumn("P_NATION", Types.VARCHAR));
      }});
      builder.put("SALES", sales);

      QuarkTable sales_greater0610215 = new QuarkTable(new ArrayList<QuarkColumn>() {{
        add(new QuarkColumn("P_SALESID", Types.INTEGER));
        add(new QuarkColumn("P_PRODUCTKEY", Types.INTEGER));
        add(new QuarkColumn("P_SALEDATE", Types.DATE));
        add(new QuarkColumn("P_NATION", Types.VARCHAR));
      }});
      builder.put("SALES_greater0610215", sales_greater0610215);

      return builder.build();
    }
  }

  public static class TpchViewSchema extends MetadataSchema {
    TpchViewSchema() {}

    @Override
    public void initialize(QueryContext queryContext) throws QuarkException {
      this.cubes = ImmutableList.of();
      ImmutableList.Builder<QuarkView> viewHolderBuilder =
          new ImmutableList.Builder<>();

      final ImmutableList<String> tpch = ImmutableList.<String>of("TPCH");
      viewHolderBuilder.add(new QuarkView("PART_100_PART",
          "select * from tpch.part where p_size = 110", "PART_100",
            tpch, ImmutableList.<String>of("TPCH", "PART_100")));
      viewHolderBuilder.add(new QuarkView("PART_less110_part", "select * " +
          "from tpch.part where " +
          "p_size" +
              " < 110", "PART",
            tpch, ImmutableList.<String>of("TPCH", "PART_less110")));
      viewHolderBuilder.add(new QuarkView("PART_greater110_part", "select " +
          "* from tpch.part " +
          "where p_size > 110", "PART",
            tpch, ImmutableList.<String>of("TPCH", "PART_greater110")));
      viewHolderBuilder.add(new QuarkView("PART_RETAILPRICE_PART", "select" +
          " * from tpch.part " +
          "where P_RETAILPRICE > 99.99", "PART",
            tpch, ImmutableList.<String>of("TPCH", "PART_RETAILPRICE")));
      viewHolderBuilder.add(new QuarkView("SALES_greater0610215_part",
          "select * from " +
              "tpch.sales where P_SALEDATE > \'2015-10-06\'", "SALES_greater0610215",
          tpch, ImmutableList.<String>of("TPCH", "SALES_greater0610215")));
      viewHolderBuilder.add(new QuarkView("PART_COMP1_PART", "select * " +
          "from tpch.part where " +
          "(P_RETAILPRICE > 19.99 OR P_PARTKEY < 100) AND P_SIZE > 70", "PART_COMP1",
          tpch, ImmutableList.<String>of("TPCH", "PART_COMP1")));

      this.views = viewHolderBuilder.build();
      super.initialize(queryContext);
    }
  }

  public static class SchemaFactory implements TestFactory {
    public List<QuarkSchema> create(Properties info) {
      return new ArrayList<QuarkSchema>() {{
        add(new DefaultSchema("default".toUpperCase()));
        add(new TpchSchema("tpch".toUpperCase()));
        add(new TpchViewSchema());
      }};
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    info = new Properties();
    info.put("unitTestMode", "true");
    info.put("schemaFactory", "com.qubole.quark.planner.test.SchemaTest$SchemaFactory");
    info.put("materializationsEnabled", "true");

    info.put("defaultSchema", QuarkTestUtil.toJson("DEFAULT"));

    log.info(info.getProperty("defaultSchema"));
  }

  @Test
  public void testSimple() throws QuarkException, SQLException {
    Parser parser = new Parser(info);
    List<String> usedTables =
        parser.getTables(parser.parse("select * from simple").getRelNode());

    assertThat(usedTables).contains("DEFAULT.SIMPLE");
  }

  @Test
  public void testDefaultSimple() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select * from default.simple",
        info,
        "SELECT * FROM DEFAULT.SIMPLE");
  }

  @Test
  public void testTpchNation() throws QuarkException, SQLException {
    Parser parser = new Parser(info);
    RelNode relNode = parser.parse("select * from tpch.nation").getRelNode();
    List<String> usedTables = parser.getTables(relNode);

    assertThat(usedTables).contains("TPCH.NATION");
  }

  @Test
  public void testTpchAsDefault() throws QuarkException, SQLException, JsonProcessingException {
    info.put("defaultSchema", QuarkTestUtil.toJson("TPCH"));
    try {
      Parser parser = new Parser(info);
      RelNode relNode = parser.parse("select * from nation").getRelNode();
      List<String> usedTables = parser.getTables(relNode);
      assertThat(usedTables).contains("TPCH.NATION");
    } finally {
      info.put("defaultSchema", QuarkTestUtil.toJson("DEFAULT"));
    }
  }

  @Test
  public void testDefaultNation() throws QuarkException, SQLException {
    Parser parser = new Parser(info);
    RelNode relNode = parser.parse("select * from tpch.nation").getRelNode();
    List<String> usedTables = parser.getTables(relNode);

    assertThat(usedTables).contains("TPCH.NATION");
  }

  //TODO: Check this regression
  @Ignore("Regression in calcite. Need to check")
  @Test
  public void testView100() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select p_name from tpch.part where p_size = 110",
        info,
        "SELECT P_NAME FROM TPCH.PART_100");
  }

  @Test
  public void testViewPartless110() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select p_name from tpch.part where p_size < 110",
        info,
        "SELECT P_NAME FROM TPCH.PART_less110");
  }

  @Test
  public void testViewGreater110() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select p_name from tpch.part where p_size > 110",
        info,
        "SELECT P_NAME FROM TPCH.PART_greater110");
  }

  @Test
  public void testViewGreater120() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select p_name from tpch.part where p_size > 120",
        info,
        "SELECT P_NAME FROM TPCH.PART_greater110 WHERE P_SIZE > 120");
  }

  @Test
  public void testViewSALES_greaterDate() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select p_salesid from tpch.sales where p_saledate > \'2016-10-07\'",
        info,
        "SELECT P_SALESID FROM TPCH.SALES_greater0610215 WHERE P_SALEDATE > '2016-10-07'");

  }

  @Test
  public void testViewPART_RETAILPRICE() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedSql(
        "select p_name from tpch.part where p_retailprice > 110.0",
        info,
        "SELECT P_NAME FROM TPCH.PART_RETAILPRICE WHERE P_RETAILPRICE > 110.0");

  }

  @Test
  public void testViewPART_COMP1_test1() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where (p_retailprice > 20.0 AND p_size > 75)",
        info,
        ImmutableList.of(TpchSchema.part_comp1_rel),
        ImmutableList.<String>of());
  }


  @Test
  public void testViewPART_COMP1_test2() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where (20.0 < p_retailprice  AND 80 < p_size)",
        info,
        ImmutableList.of(TpchSchema.part_comp1_rel),
        ImmutableList.<String>of());
  }

  @Test
  public void testViewPART_COMP1_test3() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where (20.0 < p_retailprice  AND 80 < p_size " +
            "AND p_name = 'quark')",
        info,
        ImmutableList.of(TpchSchema.part_comp1_rel),
        ImmutableList.<String>of());
  }

  @Test
  public void testViewPART_COMP1_NegativeTest1() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where (p_retailprice > 20.0 OR p_size > 500)",
        info,
        ImmutableList.of(TpchSchema.part_rel),
        ImmutableList.of(TpchSchema.part_comp1_rel));
  }

  @Test
  public void testViewPART_COMP1_NegativeTest2() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where (p_retailprice > (2.0 + 10.0) OR p_size > 500)",
        info,
        ImmutableList.of(TpchSchema.part_rel),
        ImmutableList.of(TpchSchema.part_comp1_rel));
  }

  @Test
  public void testViewPART_COMP1_NegativeTest3() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where p_size < 500",
        info,
        ImmutableList.of(TpchSchema.part_rel),
        ImmutableList.of(TpchSchema.part_comp1_rel));
  }

  @Test
  public void testViewPART_COMP1_NegativeTest4() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where p_size < 500 AND p_retailprice > 30.0",
        info,
        ImmutableList.of(TpchSchema.part_rel),
        ImmutableList.of(TpchSchema.part_comp1_rel));
  }

  @Test
  public void testViewPART_COMP1_NegativeTest5() throws QuarkException, SQLException {
    QuarkTestUtil.checkParsedRelString(
        "select p_name from tpch.part where (p_size < 500 AND p_retailprice > 30.0 AND p_name = 'qubole') ",
        info,
        ImmutableList.of(TpchSchema.part_rel),
        ImmutableList.of(TpchSchema.part_comp1_rel));
  }
}
