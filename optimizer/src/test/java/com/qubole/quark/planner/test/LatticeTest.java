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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.QuarkCube;
import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.Parser;
import com.qubole.quark.planner.TestFactory;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;
import com.qubole.quark.sql.QueryContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 3/19/15.
 */
public class LatticeTest {
  private static final Logger log = LoggerFactory.getLogger(LatticeTest.class);
  private static Properties info;

  public static class CubeSchema extends MetadataSchema {
    CubeSchema() {}

    public QuarkCube webReturnsCube() {
      final List<QuarkCube.Measure> measures = new ImmutableList.Builder<QuarkCube.Measure>()
          .add(new QuarkCube.Measure("sum", "wr_net_loss".toUpperCase(), "TOTAL_NET_LOSS"))
          .build();


      final ImmutableList<QuarkCube.Dimension> dimensions = new ImmutableList.Builder<QuarkCube.Dimension>()
          .add(new QuarkCube.Dimension(ImmutableList.of("I", "I_ITEM_ID"), "I_ITEM_ID", 0))
          .add(new QuarkCube.Dimension(ImmutableList.of("CD", "CD_GENDER"), "CD_GENDER", 5))
          .add(new QuarkCube.Dimension(ImmutableList.of("CD", "CD_MARITAL_STATUS"),
              "CD_MARITAL_STATUS", 6))
          .add(new QuarkCube.Dimension(ImmutableList.of("CD", "CD_EDUCATION_STATUS"),
              "CD_EDUCATION_STATUS", 7))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_YEAR"), "D_YEAR", 1))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_QOY"), "D_QOY", 2))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_MOY"), "D_MOY", 3))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_DATE"), "D_DATE", 4))
          .build();

      final QuarkCube count_fact = new QuarkCube("web_returns_cube",
          "select 1 from tpcds.web_returns as w " +
              "join tpcds.item as i on w.wr_item_sk = i.i_item_sk " +
              "join tpcds.customer as c on w.wr_refunded_cdemo_sk = c.c_customer_sk " +
              "join tpcds.date_dim as dd on w.wr_returned_date_sk = dd.d_date_sk " +
              "join tpcds.customer_demographics cd on c.c_current_cdemo_sk = cd.cd_demo_sk",
          measures, dimensions, ImmutableList.of("TPCDS", "WEB_RETURNS_CUBE"), "GROUPING_ID");

      return count_fact;
    }

    public QuarkCube storeSalesCube() {
      final List<QuarkCube.Measure> measures = new ImmutableList.Builder<QuarkCube.Measure>()
          .add(new QuarkCube.Measure("sum", "ss_ext_sales_price".toUpperCase(),
              "sum_extended_sales_price".toUpperCase()))
          .add(new QuarkCube.Measure("sum", "ss_sales_price".toUpperCase(),
              "sum_sales_price".toUpperCase()))
          .build();

      final ImmutableList<QuarkCube.Dimension> dimensions = new ImmutableList.Builder<QuarkCube.Dimension>()
          .add(new QuarkCube.Dimension(ImmutableList.of("I", "I_ITEM_ID"), "I_ITEM_ID", 0))
          .add(new QuarkCube.Dimension(ImmutableList.of("C", "C_CUSTOMER_ID"), "C_CUSTOMER_ID", 1))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_YEAR"), "D_YEAR", 2))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_QOY"), "D_QOY", 3))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_MOY"), "D_MOY", 4))
          .add(new QuarkCube.Dimension(ImmutableList.of("DD", "D_DATE"), "D_DATE", 5))

          .add(new QuarkCube.Dimension(ImmutableList.of("CD", "CD_GENDER"), "CD_GENDER", 6))
          .add(new QuarkCube.Dimension(ImmutableList.of("CD", "CD_MARITAL_STATUS"),
              "CD_MARITAL_STATUS", 7))
          .add(new QuarkCube.Dimension(ImmutableList.of("CD", "CD_EDUCATION_STATUS"),
              "CD_EDUCATION_STATUS", 8))
          .build();

      final QuarkCube count_fact = new QuarkCube("store_sales_cube",
          "select 1 from tpcds.store_sales as ss " +
              "join tpcds.item as i on ss.ss_item_sk = i.i_item_sk " +
              "join tpcds.customer as c on ss.ss_customer_sk = c.c_customer_sk " +
              "join tpcds.date_dim as dd on ss.ss_sold_date_sk = dd.d_date_sk " +
              "join tpcds.customer_demographics cd on ss.ss_cdemo_sk = cd.cd_demo_sk ",
          measures, dimensions, ImmutableList.of("TPCDS", "STORE_SALES_CUBE"), "GROUPING_ID");

      return count_fact;
    }

    public QuarkCube foodmartSalesCube() {
      final List<QuarkCube.Measure> measures = new ImmutableList.Builder<QuarkCube.Measure>()
          .add(new QuarkCube.Measure("sum", "unit_sales".toUpperCase(), "TOTAL_UNIT_SALES"))
          .add(new QuarkCube.Measure("sum", "store_sales".toUpperCase(), "TOTAL_STORE_SALES")
          ).build();


      final ImmutableList<QuarkCube.Dimension> dimensions = new ImmutableList.Builder<QuarkCube.Dimension>()
          .add(new QuarkCube.Dimension(ImmutableList.of("T", "THE_YEAR"), "THE_YEAR", 0))
          .add(new QuarkCube.Dimension(ImmutableList.of("T", "QUARTER"), "QUARTER", 1))
          .build();

      final QuarkCube count_fact = new QuarkCube("count_fact",
          "select 1 from foodmart.sales_fact_1997 as s "
              + "join foodmart.product as p using (product_id) "
              + "join foodmart.time_by_day as t using (time_id) "
              + "join foodmart.product_class as pc on p.product_class_id = pc.product_class_id",
          measures, dimensions, ImmutableList.of("FOODMART", "COUNT_FACT_TILE"), "GROUPING_ID");

      return count_fact;
    }

    @Override
    public void initialize(QueryContext queryContext) throws QuarkException {
      this.views = ImmutableList.of();
      this.cubes = ImmutableList.of(webReturnsCube(), storeSalesCube(), foodmartSalesCube());
      super.initialize(queryContext);
    }
  }

  public static class SchemaFactory implements TestFactory {
    public List<QuarkSchema> create(Properties info) {
      Foodmart foodmart = new Foodmart("foodmart".toUpperCase());
      Tpcds tpcds = new Tpcds("TPCDS");
      CubeSchema cubeSchema = new CubeSchema();
      return new ImmutableList.Builder<QuarkSchema>()
          .add(foodmart)
          .add(tpcds)
          .add(cubeSchema).build();
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    info = new Properties();
    info.put("schemaFactory", "com.qubole.quark.planner.test.LatticeTest$SchemaFactory");

    ImmutableList<String> defaultSchema = ImmutableList.of("FOODMART");
    final ObjectMapper mapper = new ObjectMapper();

    info.put("defaultSchema", mapper.writeValueAsString(defaultSchema));
  }

  @Test
  public void testSimple() throws QuarkException {
    Parser parser = new Parser(info);
    Parser.ParserResult result = parser.parse("select * from account");
    List<String> usedTables = parser.getTables(result.getRelNode());

    assertThat(usedTables).contains("FOODMART.ACCOUNT");
  }

  @Test
  public void testCountFact() throws QuarkException {
    final String sql = "select t.the_year, "
        + "  sum(s.unit_sales)  "
        + "from foodmart.sales_fact_1997 as s "
        + "join foodmart.time_by_day as t using (time_id) "
        + "group by t.the_year";

    QuarkTestUtil.checkParsedSql(
        sql,
        info,
        "SELECT THE_YEAR, SUM(TOTAL_UNIT_SALES) "
            + "FROM FOODMART.COUNT_FACT_TILE "
            + "WHERE GROUPING_ID = '1' "
            + "GROUP BY THE_YEAR");
  }

  @Test
  public void testWebReturns() throws QuarkException {
    final String sql = "select dd.d_year, "
        + "  sum(wr_net_loss)  "
        + "from tpcds.web_returns as w "
        + "join tpcds.date_dim as dd on w.wr_returned_date_sk = dd.d_date_sk "
        + "group by dd.d_year";

    QuarkTestUtil.checkParsedSql(
        sql,
        info,
        "SELECT D_YEAR, SUM(TOTAL_NET_LOSS) "
            + "FROM TPCDS.WEB_RETURNS_CUBE "
            + "WHERE GROUPING_ID = '2' "
            + "GROUP BY D_YEAR");
  }

  @Test
  public void storeFilterQuery() throws QuarkException {
    String sql = "select d_year, d_qoy, cd_gender, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
        "where cd_gender = 'M' group by d_year, d_qoy, cd_gender";

    QuarkTestUtil.checkParsedSql(
        sql,
        info,
        "SELECT D_YEAR, D_QOY, CD_GENDER, SUM(SUM_SALES_PRICE) " +
            "FROM TPCDS.STORE_SALES_CUBE " +
            "WHERE GROUPING_ID = '76' AND CD_GENDER = 'M' " +
            "GROUP BY D_YEAR, D_QOY, CD_GENDER");
  }

  /*
   * In this test, the filter column is not in the select list. So a sub-query with an
   * aggregate is used to remove the filter column.
   */
  @Test
  public void storeFilterQuery2() throws QuarkException {
      String sql = "select d_year, d_qoy, sum(ss_sales_price) " +
          " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
          " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
          "where cd_gender = 'M' group by d_year, d_qoy";

    // This is produced from:
    // EnumerableAggregate(group=[{0, 1}], EXPR$2=[SUM($3)]): rowcount = 1.0, cumulative cost = {3.25 rows, 40.0 cpu, 0.0 io}, id = 92536
    //   EnumerableAggregate(group=[{3, 5, 6}], EXPR$2=[SUM($12)]): rowcount = 1.0, cumulative cost = {2.125 rows, 40.0 cpu, 0.0 io}, id = 92534
    //     EnumerableCalc(expr#0..15=[{inputs}], expr#16=['76'], ...
    //       QuarkTileScan(table=[[PUBLIC, STORE_SALES_CUBE]], fields=[...

    QuarkTestUtil.checkParsedSql(
        sql,
        info,
        "SELECT D_YEAR, D_QOY, SUM(SUM(SUM_SALES_PRICE)) " +
            "FROM (SELECT D_YEAR, D_QOY, CD_GENDER, SUM(SUM_SALES_PRICE) " +
            "FROM TPCDS.STORE_SALES_CUBE WHERE GROUPING_ID = '76' AND CD_GENDER = 'M' " +
            "GROUP BY D_YEAR, D_QOY, CD_GENDER) AS t0 " +
            "GROUP BY D_YEAR, D_QOY");
  }
}
