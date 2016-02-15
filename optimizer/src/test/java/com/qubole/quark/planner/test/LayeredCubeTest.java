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
import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.Parser;
import com.qubole.quark.planner.QuarkCube;
import com.qubole.quark.planner.QuarkCube.Dimension;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.TestFactory;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;
import com.qubole.quark.sql.QueryContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by rajatv on 3/19/15.
 */
public class LayeredCubeTest {
  private static final Logger log = LoggerFactory.getLogger(LayeredCubeTest.class);
  private static Parser parser;

  public static class CubeSchema extends MetadataSchema {
    CubeSchema() {}
    private static final ImmutableList<QuarkCube.Measure> measures =
        new ImmutableList.Builder<QuarkCube.Measure>()
        .add(new QuarkCube.Measure("sum", "ss_ext_sales_price".toUpperCase(),
            "sum_extended_sales_price".toUpperCase()))
        .add(new QuarkCube.Measure("sum", "ss_sales_price".toUpperCase(),
            "sum_sales_price".toUpperCase()))
        .build();

    private static String sql = "select 1 from tpcds.store_sales as ss " +
        "join tpcds.item as i on ss.ss_item_sk = i.i_item_sk " +
        "join tpcds.customer as c on ss.ss_customer_sk = c.c_customer_sk " +
        "join tpcds.date_dim as dd on ss.ss_sold_date_sk = dd.d_date_sk " +
        "join tpcds.customer_demographics cd on ss.ss_cdemo_sk = cd.cd_demo_sk ";

    /*
     * Build a cube that has daily aggregations from Sep 1 2015 to Dec 31 2015
     */
    public QuarkCube storeSalesCubeDaily() {
      ImmutableList<QuarkCube.Measure> measuresLocal = ImmutableList.copyOf(measures);
      ImmutableList<Dimension> dimensions = new ImmutableList.Builder<Dimension>()
          .add(Dimension.builder("D_YEAR", "", "DD", "D_YEAR" , "D_YEAR", 0).setMandatory(true)
              .build())
          .add(Dimension.builder("D_MOY", "", "DD", "D_MOY" , "D_MOY", 1).setMandatory(true).build())
          .add(Dimension.builder("D_DOM", "", "DD", "D_DOM", "D_DOM", 2).setMandatory(true)
              .build())
          .add(Dimension.builder("CD_GENDER", "", "CD", "CD_GENDER",
              "CD_GENDER", 3).build())
          .build();

      final QuarkCube count_fact = new QuarkCube("store_sales_cube_daily",
          sql + "where dd.D_YEAR = 2015 and dd.D_MOY >= 9 and dd.D_MOY <= 12",
          measuresLocal, dimensions, ImmutableList.of("TPCDS", "STORE_SALES_CUBE_DAILY"),
          "GROUPING_ID");
      return count_fact;
    }

    /*
     * Build a cube that has daily aggregations from Jun 1 2015 to Aug 31 2015
     */
    public QuarkCube storeSalesCubeWeekly() {
      ImmutableList<QuarkCube.Measure> measuresLocal = ImmutableList.copyOf(measures);
      ImmutableList<Dimension> dimensions = new ImmutableList.Builder<Dimension>()
          .add(Dimension.builder("D_YEAR", "", "DD", "D_YEAR" , "D_YEAR", 0).setMandatory(true)
              .build())
          .add(Dimension.builder("D_MOY", "", "DD", "D_MOY" , "D_MOY", 1).setMandatory(true).build())
          .add(Dimension.builder("D_WEEK_SEQ", "", "DD", "D_WEEK_SEQ", "D_WEEK_SEQ", 2).setMandatory
              (true).build())
          .add(Dimension.builder("CD_GENDER", "", "CD", "CD_GENDER",
              "CD_GENDER", 3).build())
          .build();

      final QuarkCube count_fact = new QuarkCube("store_sales_cube_weekly",
          sql + "where dd.D_YEAR = 2015 and dd.D_MOY >= 6 and dd.D_MOY <= 8",
          measuresLocal, dimensions, ImmutableList.of("TPCDS", "STORE_SALES_CUBE_WEEKLY"),
          "GROUPING_ID");
      return count_fact;
    }

    /*
     * Build a cube that has monthly aggregations from Jan 1 2015 to May 31 2015
     */
    public QuarkCube storeSalesCubeMonthly() {
      ImmutableList<QuarkCube.Measure> measuresLocal = ImmutableList.copyOf(measures);
      ImmutableList<Dimension> dimensions = new ImmutableList.Builder<Dimension>()
          .add(Dimension.builder("D_YEAR", "", "DD", "D_YEAR", "D_YEAR", 0).setMandatory(true)
              .build())
          .add(Dimension.builder("D_MOY", "", "DD", "D_MOY" , "D_MOY", 1).setMandatory(true).build())
          .add(Dimension.builder("CD_GENDER", "", "CD", "CD_GENDER",
              "CD_GENDER", 2).build())
          .build();

      final QuarkCube count_fact = new QuarkCube("store_sales_cube_monthly",
          sql + "where dd.D_YEAR = 2015 and dd.D_MOY >= 1 and dd.D_MOY <= 5",
          measuresLocal, dimensions, ImmutableList.of("TPCDS", "STORE_SALES_CUBE_MONTHLY"),
          "GROUPING_ID");
      return count_fact;
    }

    @Override
    public void initialize(QueryContext queryContext) throws QuarkException {
      this.views = ImmutableList.of();
      this.cubes = ImmutableList.of(storeSalesCubeDaily(), storeSalesCubeWeekly(), storeSalesCubeMonthly());
      super.initialize(queryContext);
    }
  }

  public static class SchemaFactory implements TestFactory {
    public List<QuarkSchema> create(Properties info) {
      Tpcds tpcds = new Tpcds("TPCDS");
      CubeSchema cubeSchema = new CubeSchema();
      return new ImmutableList.Builder<QuarkSchema>()
          .add(tpcds)
          .add(cubeSchema).build();
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    Properties info = new Properties();
    info.put("unitTestMode", "true");
    info.put("schemaFactory", "com.qubole.quark.planner.test.LayeredCubeTest$SchemaFactory");

    ImmutableList<String> defaultSchema = ImmutableList.of("TPCDS");
    final ObjectMapper mapper = new ObjectMapper();

    info.put("defaultSchema", mapper.writeValueAsString(defaultSchema));
    parser = new Parser(info);
  }

  @Test
  public void aggSingleDayDec() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, d_dom, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 12 and d_dom = 1 group by d_year, d_moy, d_dom";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT D_YEAR, D_MOY, D_DOM, SUM(SUM_SALES_PRICE) " +
            "FROM TPCDS.STORE_SALES_CUBE_DAILY WHERE D_YEAR = 2015 AND D_MOY = 12 AND D_DOM = 1 " +
            "AND GROUPING_ID = '7' GROUP BY D_YEAR, D_MOY, D_DOM");
  }

  @Test
  public void aggWeekDec() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, d_dom, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 12 and d_dom >= 1 and d_dom <= 7 group by d_year, d_moy," +
        " d_dom";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT D_YEAR, D_MOY, D_DOM, SUM(SUM_SALES_PRICE) " +
            "FROM TPCDS.STORE_SALES_CUBE_DAILY WHERE D_YEAR = 2015 AND D_MOY = 12 AND " +
            "D_DOM >= 1 AND D_DOM <= 7 " +
            "AND GROUPING_ID = '7' GROUP BY D_YEAR, D_MOY, D_DOM");
  }

  @Test
  public void aggMonthDec() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 12 group by d_year, d_moy";

    QuarkTestUtil.checkParsedRelString(
        sql,
        parser,
        ImmutableList.of("STORE_SALES_CUBE_DAILY"),
        ImmutableList.of("DATE_DIM"));
  }

  @Test
  public void aggSingleDayJul() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, d_dom, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 7 and d_dom = 1 group by d_year, d_moy, d_dom";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT t.D_YEAR, t.D_MOY, t.D_DOM, SUM(STORE_SALES.SS_SALES_PRICE) " +
            "FROM (" +
            "SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2015 AND D_MOY = 7 AND D_DOM = 1" +
            ") AS t " +
            "INNER JOIN TPCDS.STORE_SALES ON t.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK " +
            "GROUP BY t.D_YEAR, t.D_MOY, t.D_DOM");
  }

  @Test
  public void aggWeekJul() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, d_week_seq, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 7 and d_week_seq = 27 group by d_year, d_moy, d_week_seq";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT D_YEAR, D_MOY, D_WEEK_SEQ, SUM(SUM_SALES_PRICE) " +
            "FROM TPCDS.STORE_SALES_CUBE_WEEKLY WHERE D_YEAR = 2015 AND " +
            "D_MOY = 7 AND D_WEEK_SEQ = 27 AND GROUPING_ID = '7' " +
            "GROUP BY D_YEAR, D_MOY, D_WEEK_SEQ");
  }

  /**
   * Rollup is done when tile with exact dimensions is not found.
   */
  @Test
  public void aggMonthJul() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 7 group by d_year, d_moy";

    QuarkTestUtil.checkParsedRelString(
        sql,
        parser,
        ImmutableList.of("STORE_SALES_CUBE_WEEKLY"),
        ImmutableList.of("DATE_DIM"));
  }

  @Test
  public void aggSingleDayFeb() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, d_dom, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 2 and d_dom = 1 group by d_year, d_moy, d_dom";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT t.D_YEAR, t.D_MOY, t.D_DOM, SUM(STORE_SALES.SS_SALES_PRICE)" +
            " FROM (" +
            "SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2015 AND D_MOY = 2 AND D_DOM = 1" +
            ") AS t " +
            "INNER JOIN TPCDS.STORE_SALES ON t.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK " +
            "GROUP BY t.D_YEAR, t.D_MOY, t.D_DOM");
  }

  @Test
  public void aggWeekFeb() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, d_week_seq, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 2 and d_week_seq = 6 group by d_year, d_moy, d_week_seq";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT t.D_YEAR, t.D_MOY, t.D_WEEK_SEQ, SUM(STORE_SALES.SS_SALES_PRICE) " +
            "FROM (SELECT * FROM TPCDS.DATE_DIM " +
            "WHERE D_YEAR = 2015 AND D_MOY = 2 AND D_WEEK_SEQ = 6) AS t " +
            "INNER JOIN TPCDS.STORE_SALES ON t.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK " +
            "GROUP BY t.D_WEEK_SEQ, t.D_YEAR, t.D_MOY");
  }

  @Test
  public void aggMonthFeb() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 2 group by d_year, d_moy";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT D_YEAR, D_MOY, SUM(SUM_SALES_PRICE) " +
            "FROM TPCDS.STORE_SALES_CUBE_MONTHLY WHERE D_YEAR = 2015 AND D_MOY = 2 " +
            "AND GROUPING_ID = '3' " +
            "GROUP BY D_YEAR, D_MOY");
  }

  @Test
  public void aggDateDecNoMatch() throws QuarkException, SQLException {
    String sql = "select d_date, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy = 12 group by d_date";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT t.D_DATE, SUM(STORE_SALES.SS_SALES_PRICE) " +
            "FROM (SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2015 AND D_MOY = 12) AS t " +
            "INNER JOIN TPCDS.STORE_SALES ON t.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK " +
            "GROUP BY t.D_DATE");
  }

  @Test
  public void aggMonthDec2016NoMatch() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2016 and d_moy = 12 group by d_year, d_moy";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT t.D_YEAR, t.D_MOY, SUM(STORE_SALES.SS_SALES_PRICE) " +
            "FROM (SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2016 AND D_MOY = 12) AS t " +
            "INNER JOIN TPCDS.STORE_SALES ON t.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK " +
            "GROUP BY t.D_YEAR, t.D_MOY");
  }

  @Test
  public void aggSpanCubes() throws QuarkException, SQLException {
    String sql = "select d_year, d_moy, sum(ss_sales_price) " +
        " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
        "where d_year = 2015 and d_moy >= 8 and d_moy <= 12 group by d_year, d_moy";

    QuarkTestUtil.checkParsedSql(
        sql,
        parser,
        "SELECT t.D_YEAR, t.D_MOY, SUM(STORE_SALES.SS_SALES_PRICE) " +
            "FROM (SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2015 AND D_MOY >= 8 AND D_MOY <= " +
            "12) AS t INNER JOIN TPCDS.STORE_SALES ON t.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK " +
            "GROUP BY t.D_YEAR, t.D_MOY");
  }

}
