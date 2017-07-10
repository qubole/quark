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
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableList;

import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by rajatv on 3/19/15.
 */
@RunWith(Enclosed.class)
public class PartialCubeTest {
  private static SqlQueryParser parser;

  protected static SqlQueryParser getParser(String filter) throws JsonProcessingException, QuarkException {
    Properties info = new Properties();
    info.put("unitTestMode", "true");
    info.put("schemaFactory", "com.qubole.quark.planner.test.PartialCubeSchemaFactory");

    ImmutableList<String> defaultSchema = ImmutableList.of("TPCDS");
    final ObjectMapper mapper = new ObjectMapper();

    info.put("defaultSchema", mapper.writeValueAsString(defaultSchema));
    info.put("filter", filter);
    return new SqlQueryParser(info);
  }

  public static class SingleDayFilter {
    @BeforeClass
    public static void setUpClass() throws Exception {
      parser = PartialCubeTest.getParser("where dd.D_YEAR = 2007 and dd.D_MOY=1 and dd.D_DOM=1");
    }

    @Test
    public void singleDayFilterMatch() throws QuarkException, SQLException {
      String sql = "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
          " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
          " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
          "where D_YEAR = 2007 and D_MOY=1 and D_DOM=1 group by " +
          "d_year, d_moy, d_dom, cd_gender";

      QuarkTestUtil.checkParsedSql(
          sql,
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND D_YEAR = 2007 AND " +
              "D_MOY = 1 AND D_DOM = 1 GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");
    }

    @Test
    public void singleDayFilterNoMatch() throws QuarkException, SQLException {
      String sql = "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
          " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
          " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
          "where D_YEAR = 2007 and D_MOY=1 and D_DOM=2 group by " +
          "d_year, d_moy, d_dom, cd_gender";

      QuarkTestUtil.checkParsedSql(
          sql,
          parser,
          "SELECT t.D_YEAR, t.D_MOY, t.D_DOM, CUSTOMER_DEMOGRAPHICS.CD_GENDER, " +
              "SUM(STORE_SALES.SS_SALES_PRICE) FROM TPCDS.STORE_SALES " +
              "INNER JOIN (SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2007 " +
              "AND D_MOY = 1 AND D_DOM = 2) AS t " +
              "ON STORE_SALES.SS_SOLD_DATE_SK = t.D_DATE_SK " +
              "INNER JOIN TPCDS.CUSTOMER_DEMOGRAPHICS " +
              "ON STORE_SALES.SS_CDEMO_SK = CUSTOMER_DEMOGRAPHICS.CD_DEMO_SK " +
              "GROUP BY t.D_YEAR, t.D_MOY, t.D_DOM, CUSTOMER_DEMOGRAPHICS.CD_GENDER");
    }
  }

  public static class InClauseFilter {
    @BeforeClass
    public static void setUpClass() throws Exception {
      parser = PartialCubeTest.getParser("where dd.D_YEAR = 2007 and dd.D_MOY=1 and dd.D_DOM " +
          "in (1, 2, 3, 4)");
    }

    @Test
    public void day1FilterMatch() throws QuarkException, SQLException {
      String sql = "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
          " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
          " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
          "where D_YEAR = 2007 and D_MOY=1 and D_DOM = 2 group by " +
          "d_year, d_moy, d_dom, cd_gender";


      QuarkTestUtil.checkParsedSql(
          sql,
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND D_YEAR = 2007 AND " +
              "D_MOY = 1 AND D_DOM = 2 GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");
    }

    @Test
    public void day2FilterMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where D_YEAR = 2007 and D_MOY=1 and D_DOM = 1 group by " +
              "d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND D_YEAR = 2007 AND " +
              "D_MOY = 1 AND D_DOM = 1 GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");

    }

    @Test
    public void day2and3FilterMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where D_YEAR = 2007 and D_MOY=1 and D_DOM in (2, 3) group by " +
              "d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND " +
              "D_YEAR = 2007 AND D_MOY = 1 AND (D_DOM = 2 OR D_DOM = 3)" +
              " GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");

    }

    @Test
    public void day2and3GroupByGenderMaritalMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedRelString(
          "select cd_gender, cd_marital_status, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where D_YEAR = 2007 and D_MOY=1 and D_DOM in (2, 3) group by " +
              "cd_gender, cd_marital_status",
          parser,
          ImmutableList.of("STORE_SALES_CUBE_PARTIAL"),
          ImmutableList.of("CUSTOMER_DEMOGRAPHICS"));
    }
  }

  public static class RangeDayFilter {
    @BeforeClass
    public static void setUpClass() throws Exception {
      parser = PartialCubeTest.getParser("where dd.D_YEAR = 2007 and dd.D_MOY=1 and dd.D_DOM >= 1" +
          " and dd.D_DOM <= 25");
    }

    @Test
    public void day14FilterMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where D_YEAR = 2007 and D_MOY=1 and D_DOM = 14 group by " +
              "d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND D_YEAR = 2007 AND " +
              "D_MOY = 1 AND D_DOM = 14 GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");
    }

    @Test
    public void day25FilterMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where D_YEAR = 2007 and D_MOY=1 and D_DOM = 25 group by " +
              "d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND D_YEAR = 2007 AND " +
              "D_MOY = 1 AND D_DOM = 25 GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");
    }

    @Test
    public void day20GroupByGenderMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedRelString(
          "select cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where D_YEAR = 2007 and D_MOY=1 and D_DOM = 25 group by " +
              "cd_gender",
          parser,
          ImmutableList.of("STORE_SALES_CUBE"),
          ImmutableList.of("CUSTOMER_DEMOGRAPHICS"));
    }
  }

  public static class ComplexDayFilter {
    @BeforeClass
    public static void setUpClass() throws Exception {
      parser = PartialCubeTest.getParser("where dd.D_YEAR = 2007 and " +
          "((dd.D_MOY=2 and dd.D_DOM >= 1 and dd.D_DOM <= 20) or " +
          "(dd.D_MOY=1 and dd.D_DOM >=1 and dd.D_MOY <= 15))");
    }

    @Test
    public void month1day14ORmonth2day18FilterMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where (D_YEAR = 2007 and D_MOY=1 and D_DOM = 14) " +
              "or (D_YEAR = 2007 and D_MOY=2 and D_DOM = 18) " +
              "group by d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL " +
              "WHERE GROUPING_ID = '60' AND (D_YEAR = 2007 AND D_MOY = 1 AND D_DOM = 14 " +
              "OR D_YEAR = 2007 AND D_MOY = 2 AND D_DOM = 18) " +
              "GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");
    }

    @Test
    public void month1day2ORmonth2day22FilterNoMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where (D_YEAR = 2007 and D_MOY=1 and D_DOM = 2) " +
              "or (D_YEAR = 2007 and D_MOY=2 and D_DOM = 22) " +
              "group by d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT t.D_YEAR, t.D_MOY, t.D_DOM, CUSTOMER_DEMOGRAPHICS.CD_GENDER, " +
              "SUM(STORE_SALES.SS_SALES_PRICE) FROM TPCDS.STORE_SALES " +
              "INNER JOIN (SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2007 " +
              "AND D_MOY = 1 AND D_DOM = 2 OR D_YEAR = 2007 AND D_MOY = 2 " +
              "AND D_DOM = 22) AS t " +
              "ON STORE_SALES.SS_SOLD_DATE_SK = t.D_DATE_SK " +
              "INNER JOIN TPCDS.CUSTOMER_DEMOGRAPHICS " +
              "ON STORE_SALES.SS_CDEMO_SK = CUSTOMER_DEMOGRAPHICS.CD_DEMO_SK " +
              "GROUP BY t.D_YEAR, t.D_MOY, t.D_DOM, CUSTOMER_DEMOGRAPHICS.CD_GENDER");
    }
  }

  public static class RangeDayFilterMisc {
    @BeforeClass
    public static void setUpClass() throws Exception {
      parser = PartialCubeTest.getParser("where dd.D_YEAR = 2007 and dd.D_MOY=1 and dd.D_DOM >= 1" +
          " and dd.D_DOM <= 25");
    }

    /*
     * Test query has filter on non-dimension column *not* in the select list.
     * Even if cube filters are satisfied in by below query, the query cannot be
     * optimized when filter is on non-dimension
     */
    @Test
    public void nonDimensionFilterNoMatch() throws QuarkException, SQLException {
      String sql = "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
          " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
          " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
          "where D_YEAR = 2007 and D_MOY=1 and D_DOM = 14 OR D_DATE = '14-01-2007' group by " +
          "d_year, d_moy, d_dom, cd_gender";
      QuarkTestUtil.checkParsedSql(
          sql,
          parser,
          "SELECT t.D_YEAR, t.D_MOY, t.D_DOM, CUSTOMER_DEMOGRAPHICS.CD_GENDER, " +
              "SUM(STORE_SALES.SS_SALES_PRICE) FROM TPCDS.STORE_SALES " +
              "INNER JOIN (SELECT * FROM TPCDS.DATE_DIM WHERE D_YEAR = 2007 " +
              "AND D_MOY = 1 AND D_DOM = 14 OR D_DATE = '14-01-2007') AS t " +
              "ON STORE_SALES.SS_SOLD_DATE_SK = t.D_DATE_SK " +
              "INNER JOIN TPCDS.CUSTOMER_DEMOGRAPHICS " +
              "ON STORE_SALES.SS_CDEMO_SK = CUSTOMER_DEMOGRAPHICS.CD_DEMO_SK " +
              "GROUP BY t.D_YEAR, t.D_MOY, t.D_DOM, " +
              "CUSTOMER_DEMOGRAPHICS.CD_GENDER");
    }

    @Test
    public void noFilterNoMatch() throws QuarkException, SQLException {
      String sql = "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
          " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
          " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk" +
          " group by d_year, d_moy, d_dom, cd_gender";
      QuarkTestUtil.checkParsedSql(
          sql,
          parser,
          "SELECT DATE_DIM.D_YEAR, DATE_DIM.D_MOY, DATE_DIM.D_DOM, " +
              "CUSTOMER_DEMOGRAPHICS.CD_GENDER, SUM(STORE_SALES.SS_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES INNER JOIN TPCDS.DATE_DIM " +
              "ON STORE_SALES.SS_SOLD_DATE_SK = DATE_DIM.D_DATE_SK " +
              "INNER JOIN TPCDS.CUSTOMER_DEMOGRAPHICS " +
              "ON STORE_SALES.SS_CDEMO_SK = CUSTOMER_DEMOGRAPHICS.CD_DEMO_SK " +
              "GROUP BY DATE_DIM.D_YEAR, DATE_DIM.D_MOY, DATE_DIM.D_DOM, " +
              "CUSTOMER_DEMOGRAPHICS.CD_GENDER");
    }
  }

  public static class filterOnNonMandatory {
    @BeforeClass
    public static void setUpClass() throws Exception {
      parser = PartialCubeTest.getParser("where CD_GENDER = 'M'");
    }

    @Test
    public void filterAndDimensionMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where CD_GENDER = 'M' group by " +
              "d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, CD_GENDER, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE GROUPING_ID = '60' AND CD_GENDER = 'M'" +
              " GROUP BY D_YEAR, D_MOY, D_DOM, CD_GENDER");
    }

    @Test
    public void filterOnlyMatch() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where CD_GENDER = 'M' group by " +
              "d_year, d_moy, d_dom",
          parser,
          "SELECT D_YEAR, D_MOY, D_DOM, SUM(SUM_SALES_PRICE) " +
              "FROM TPCDS.STORE_SALES_CUBE_PARTIAL WHERE CD_GENDER = 'M'" +
              " AND GROUPING_ID = '28' GROUP BY D_YEAR, D_MOY, D_DOM");
    }

    @Test
    public void dimensionOnly() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "where CD_GENDER = 'F' group by " +
              "d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT DATE_DIM.D_YEAR, DATE_DIM.D_MOY, DATE_DIM.D_DOM, "
              + "t.CD_GENDER, SUM(STORE_SALES.SS_SALES_PRICE) "
              + "FROM TPCDS.STORE_SALES INNER JOIN "
              + "(SELECT * FROM TPCDS.CUSTOMER_DEMOGRAPHICS WHERE CD_GENDER = 'F') AS t "
              + "ON STORE_SALES.SS_CDEMO_SK = t.CD_DEMO_SK "
              + "INNER JOIN TPCDS.DATE_DIM ON STORE_SALES.SS_SOLD_DATE_SK = DATE_DIM.D_DATE_SK "
              + "GROUP BY t.CD_GENDER, DATE_DIM.D_YEAR, DATE_DIM.D_MOY, DATE_DIM.D_DOM");
    }

    @Test
    public void noFilter() throws QuarkException, SQLException {
      QuarkTestUtil.checkParsedSql(
          "select d_year, d_moy, d_dom, cd_gender, sum(ss_sales_price) " +
              " from tpcds.store_sales join tpcds.date_dim on ss_sold_date_sk = d_date_sk " +
              " join tpcds.customer_demographics on ss_cdemo_sk = cd_demo_sk " +
              "group by d_year, d_moy, d_dom, cd_gender",
          parser,
          "SELECT DATE_DIM.D_YEAR, DATE_DIM.D_MOY, DATE_DIM.D_DOM, " +
              "CUSTOMER_DEMOGRAPHICS.CD_GENDER, SUM(STORE_SALES.SS_SALES_PRICE) FROM " +
              "TPCDS.STORE_SALES INNER JOIN TPCDS.DATE_DIM " +
              "ON STORE_SALES.SS_SOLD_DATE_SK = DATE_DIM.D_DATE_SK " +
              "INNER JOIN TPCDS.CUSTOMER_DEMOGRAPHICS " +
              "ON STORE_SALES.SS_CDEMO_SK = CUSTOMER_DEMOGRAPHICS.CD_DEMO_SK " +
              "GROUP BY DATE_DIM.D_YEAR, DATE_DIM.D_MOY, DATE_DIM.D_DOM, " +
              "CUSTOMER_DEMOGRAPHICS.CD_GENDER");
    }
  }
}
