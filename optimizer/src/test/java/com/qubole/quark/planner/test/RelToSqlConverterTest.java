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
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.TestFactory;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;

/**
 * Created by vishalg on 2/6/15.
 */
public class RelToSqlConverterTest {

  private static final Logger log = LoggerFactory.getLogger(QueryTest.class);
  private static Properties info;

  public static class SchemaFactory implements TestFactory {
    public List<QuarkSchema> create(Properties info) {
      return new ArrayList<QuarkSchema>() {{
        add(new Foodmart("foodmart".toUpperCase()));
      }};
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    info = new Properties();
    info.put("unitTestMode", "true");
    info.put("defaultSchema", QuarkTestUtil.toJson("FOODMART"));
    info.put("schemaFactory", "com.qubole.quark.planner.test.RelToSqlConverterTest$SchemaFactory");

    ImmutableList<String> defaultSchema = ImmutableList.of("FOODMART");
    final ObjectMapper mapper = new ObjectMapper();
    info.put("defaultSchema", mapper.writeValueAsString(defaultSchema));
  }

  @Test
  public void testSimpleSelectQueryFromProductTable() throws QuarkException, SQLException {
    String query = "select product_id from product";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID FROM FOODMART.PRODUCT");
  }

  @Test
  public void testSimpleSelectQueryFromCategoryTable() throws QuarkException, SQLException {
    String query = "select category_id from category";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT CATEGORY_ID FROM FOODMART.CATEGORY");
  }

  @Test
  public void testSelectTwoColumnQuery() throws QuarkException, SQLException {
    String query = "select product_id, product_class_id from product";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, PRODUCT_CLASS_ID FROM FOODMART.PRODUCT");
  }

  //TODO: add test for query -> select * from product

  @Test
  public void testSelectQueryWithWhereClauseOfLessThan() throws QuarkException, SQLException {
    String query = "select product_id, shelf_width from product where product_id < 10";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, SHELF_WIDTH "
            + "FROM FOODMART.PRODUCT "
            + "WHERE PRODUCT_ID < 10");
  }

  //This test also produces operand product_id which is casted to integer.
  @Test
  public void testSelectQueryWithWhereClauseOfEqual() throws QuarkException, SQLException {
    String query = "select product_id, shelf_width from product where product_id = 10";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, SHELF_WIDTH "
            + "FROM FOODMART.PRODUCT WHERE PRODUCT_ID = 10");
  }

  @Test
  public void testSelectQueryWithWhereClauseofGreaterThan() throws QuarkException, SQLException {
    String query = "select product_id, shelf_width from product where product_id > 10";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, SHELF_WIDTH "
            + "FROM FOODMART.PRODUCT "
            + "WHERE PRODUCT_ID > 10");
  }

  @Test
  public void testSelectQueryWithWhereClauseofLessThanEqualTo() throws QuarkException, SQLException {
    String query = "select product_id, shelf_width from product where product_id <= 10";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, SHELF_WIDTH "
            + "FROM FOODMART.PRODUCT "
            + "WHERE PRODUCT_ID <= 10");
  }

  @Test
  public void testSelectQueryWithWhereClauseofGreaterThanEqualTo() throws QuarkException, SQLException {
    String query = "select product_id, shelf_width from product where product_id >= 10";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, SHELF_WIDTH "
            + "FROM FOODMART.PRODUCT "
            + "WHERE PRODUCT_ID >= 10");
  }

  @Test
  public void testSelectQueryWithWhereClauseWithExpressionFirst() throws QuarkException, SQLException {
    String query = "select product_id, shelf_width from product where 10 < product_id";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, SHELF_WIDTH "
            + "FROM FOODMART.PRODUCT "
            + "WHERE 10 < PRODUCT_ID");
  }

  @Test
  public void testSelectQueryWithGroupBy() throws QuarkException, SQLException {
    String query = "select count(*) from product group by product_class_id, product_id "; //having net_weight > 100";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT COUNT(*) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID, PRODUCT_ID");
  }

  @Test
  public void testSelectQueryWithMinAggregateFunction() throws QuarkException, SQLException {
    String query = "select min(net_weight) from product group by product_class_id ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT MIN(NET_WEIGHT) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID");
  }

  @Test
  public void testSelectQueryWithMinAggregateFunction1() throws QuarkException, SQLException {
    String query = "select PRODUCT_CLASS_ID, min(net_weight) from product group by product_class_id ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_CLASS_ID, MIN(NET_WEIGHT) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID");
  }

  @Test
  public void testSelectQueryWithSumAggregateFunction() throws QuarkException, SQLException {
    String query = "select sum(net_weight) from product group by product_class_id ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT SUM(NET_WEIGHT) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID");
  }

  @Test
  public void testSelectQueryWithMultipleAggregateFunction() throws QuarkException, SQLException {
    String query = "select sum(net_weight), min(low_fat), count(*) from product group by product_class_id ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT SUM(NET_WEIGHT), MIN(LOW_FAT), COUNT(*) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID");
  }

  @Test
  public void testSelectQueryWithMultipleAggregateFunction1() throws QuarkException, SQLException {
    String query = "select product_class_id, sum(net_weight), min(low_fat), count(*) from product group by product_class_id ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_CLASS_ID, SUM(NET_WEIGHT), MIN(LOW_FAT), COUNT(*) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID");
  }

  @Test
  public void testSelectQueryWithGroupByAndProjectList() throws QuarkException, SQLException {
    String query = "select product_class_id, product_id, count(*) from product group by product_class_id, product_id "; //having net_weight > 100";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_CLASS_ID, PRODUCT_ID, COUNT(*) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID, PRODUCT_ID");
  }

  @Test
  public void testSelectQueryWithGroupByAndProjectList1() throws QuarkException, SQLException {
    String query = "select count(*)  from product group by product_class_id, product_id "; //having net_weight > 100";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT COUNT(*) "
            + "FROM FOODMART.PRODUCT "
            + "GROUP BY PRODUCT_CLASS_ID, PRODUCT_ID");
  }

//  //TODO: group by having queries
//  @Test
//  public void testSelectQueryWithGroupByHaving() throws SQLException {
//    String query = "select count(*) from product group by product_class_id, product_id having product_id > 10"; //having net_weight > 100";
//    String finalQuery = getQueryFromRelNodeHelper(query);
//    assertEquals("SELECT COUNT(*), PRODUCT_CLASS_ID, PRODUCT_ID " +
//            "FROM FOODMART.PRODUCT " +
//            "GROUP BY PRODUCT_CLASS_ID, PRODUCT_ID " +
//            "HAVING PRODUCT_ID > 10", finalQuery);
//  }

  @Test
  public void testSelectQueryWithOrderByClause() throws QuarkException, SQLException {
    String query = "select product_id from product order by net_weight";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, NET_WEIGHT "
            + "FROM FOODMART.PRODUCT "
            + "ORDER BY NET_WEIGHT");
  }

  @Test
  public void testSelectQueryWithOrderByClause1() throws QuarkException, SQLException {
    String query = "select product_id, net_weight from product order by net_weight";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, NET_WEIGHT "
            + "FROM FOODMART.PRODUCT "
            + "ORDER BY NET_WEIGHT");
  }

  @Test
  public void testSelectQueryWithTwoOrderByClause() throws QuarkException, SQLException {
    String query = "select product_id from product order by net_weight, gross_weight";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, NET_WEIGHT, GROSS_WEIGHT "
            + "FROM FOODMART.PRODUCT "
            + "ORDER BY NET_WEIGHT, GROSS_WEIGHT");
  }

  @Test
  public void testSelectQueryWithTwoOrderByClause1() throws QuarkException, SQLException {
    String query = "select product_id, net_weight from product order by net_weight, gross_weight";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, NET_WEIGHT, GROSS_WEIGHT "
            + "FROM FOODMART.PRODUCT "
            + "ORDER BY NET_WEIGHT, GROSS_WEIGHT");
  }

  @Test
  public void testSelectQueryWithAscDescOrderByClause() throws QuarkException, SQLException {
    String query = "select product_id from product order by net_weight asc, gross_weight desc, low_fat";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, NET_WEIGHT, GROSS_WEIGHT, LOW_FAT "
            + "FROM FOODMART.PRODUCT "
            + "ORDER BY NET_WEIGHT, GROSS_WEIGHT DESC, LOW_FAT");
  }

  @Test
  public void testSelectQueryWithAscDescOrderByClause1() throws QuarkException, SQLException {
    String query = "select product_id, net_weight, gross_weight from product order by net_weight asc, gross_weight desc, low_fat";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID, NET_WEIGHT, GROSS_WEIGHT, LOW_FAT "
            + "FROM FOODMART.PRODUCT "
            + "ORDER BY NET_WEIGHT, GROSS_WEIGHT DESC, LOW_FAT");
  }

  @Test
  public void testSelectQueryWithLimitClause() throws QuarkException, SQLException {
    String query = "select product_id from product limit 100 offset 10";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT PRODUCT_ID "
            + "FROM FOODMART.PRODUCT "
            + "LIMIT 100 OFFSET 10");
  }

  @Test
  public void testSqlParsingOfLimitClauseForRedShift() throws QuarkException, SQLException, SqlParseException {
    String query = "select product_id from product limit 100 offset 10";
    final SqlDialect redshiftDialect =
        SqlDialect.getProduct("REDSHIFT", null).getDialect();
    redshiftDialect.setUseLimitKeyWord(true);
    QuarkTestUtil.checkSqlParsing(
        query,
        info,
        "SELECT \"PRODUCT_ID\" "
            + "FROM \"PRODUCT\" "
            + "OFFSET 10 LIMIT 100",
        redshiftDialect);
  }

  @Test
  public void testSelectQueryComplex() throws QuarkException, SQLException {
    String query = "select count(*) from product where cases_per_pallet > 100 group by product_id, units_per_case order by units_per_case desc";//, units_per_case, net_weight ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT COUNT(*), UNITS_PER_CASE "
            + "FROM FOODMART.PRODUCT "
            + "WHERE CASES_PER_PALLET > 100 "
            + "GROUP BY PRODUCT_ID, UNITS_PER_CASE "
            + "ORDER BY UNITS_PER_CASE DESC");
  }

  @Test
  public void testSelectQueryComplex1() throws QuarkException, SQLException {
    String query = "select count(*), units_per_case, product_id from product where cases_per_pallet > 100 group by product_id, units_per_case order by units_per_case desc";//, units_per_case, net_weight ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT COUNT(*), UNITS_PER_CASE, PRODUCT_ID "
            + "FROM FOODMART.PRODUCT "
            + "WHERE CASES_PER_PALLET > 100 "
            + "GROUP BY PRODUCT_ID, UNITS_PER_CASE "
            + "ORDER BY UNITS_PER_CASE DESC");
  }

  @Test
  public void testSelectQueryWithGroup() throws QuarkException, SQLException {
    String query = "select count(*), sum(employee_id) from RESERVE_EMPLOYEE where hire_date > '2015-01-01' "
        + "and (position_title = 'SDE' or position_title = 'SDM') group by store_id, position_title";//, units_per_case, net_weight ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT COUNT(*), SUM(EMPLOYEE_ID) FROM "
            + "FOODMART.RESERVE_EMPLOYEE WHERE HIRE_DATE > '2015-01-01' AND "
            + "(POSITION_TITLE = 'SDE' OR POSITION_TITLE = 'SDM') "
            + "GROUP BY POSITION_TITLE, STORE_ID");
  }

  @Test
  public void testSelectQueryWithGroup1() throws QuarkException, SQLException {
    String query = "select count(*), sum(employee_id), POSITION_TITLE, STORE_ID from RESERVE_EMPLOYEE where hire_date > '2015-01-01' "
        + "and (position_title = 'SDE' or position_title = 'SDM') group by store_id, position_title";//, units_per_case, net_weight ";
    QuarkTestUtil.checkParsedSql(
        query,
        info,
        "SELECT COUNT(*), SUM(EMPLOYEE_ID), POSITION_TITLE, STORE_ID FROM "
            + "FOODMART.RESERVE_EMPLOYEE WHERE HIRE_DATE > '2015-01-01' AND "
            + "(POSITION_TITLE = 'SDE' OR POSITION_TITLE = 'SDM') "
            + "GROUP BY POSITION_TITLE, STORE_ID");
  }
}
