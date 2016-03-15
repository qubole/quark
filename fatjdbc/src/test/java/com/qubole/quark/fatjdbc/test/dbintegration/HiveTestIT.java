package com.qubole.quark.fatjdbc.test.dbintegration;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by amoghm on 12/8/15.
 */
public class HiveTestIT {
  private final Connection conn = getConnection();

  private static Connection getConnection() {
    try {
      Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
      return DriverManager.getConnection("jdbc:quark:fat:json:"
          + System.getProperty("integrationTestResource") + "/hiveModel.json",
          new Properties());
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException("Exception thrown on creating quark connection: "
          + e.getMessage());
    }
  }

  public void tpcds500Query(String dataSource, String table) throws SQLException {
    String query = "select * from "
        + dataSource + "."
        + "tpcds_orc_500" +"."
        + table + " limit 1";
    conn.createStatement().executeQuery(query);
  }

  public void allTpcds500Queries(String dataSource) throws SQLException {
    tpcds500Query(dataSource, "customer");
    tpcds500Query(dataSource, "customer_address");
    tpcds500Query(dataSource, "customer_demographics");
    tpcds500Query(dataSource, "date_dim");
    tpcds500Query(dataSource, "household_demographics");
    tpcds500Query(dataSource, "item");
    tpcds500Query(dataSource, "promotion");
    tpcds500Query(dataSource, "store");
    tpcds500Query(dataSource, "store_sales");
    /* Instead of returning 9 values, Qubole SDK returns 8 values
    tpcds500Query(dataSource, "time_dim");*/

  }

  public void basicTypesQuery(String dataSource) throws SQLException {
  /*TABLE SETUP
    drop database if exists nezha_test;
    create database nezha_test;
    use nezha_test;
    create table basicTypes(
    a tinyint,
    b smallint,
    c int,
    d bigint,
    e float,
    f decimal,
    g decimal(9,7),
    h double,
    i char(10),
    j varchar(64),
    k string,
    l date,
    m timestamp);

    insert into table basicTypes values
        (1,2,3,4,5.63543,7.84354,9.25566,5.678, "nezhatest", "running hive test",
            "on all data types", '2015-10-25', '2015-10-25 13:15:55.123456789');*/
    String query = "select * from "
        + dataSource + "."
        + "nezha_test.basicTypes limit 1";
    conn.createStatement().executeQuery(query);
  }

  @Ignore
  @Test
  public void testTpcdsOnEMRHive() {
    try {
      allTpcds500Queries("hive");
    } catch (Exception e) {
      assertTrue("Exception thrown while executing queries on EMR hive: "
          + e.getMessage(), false);
    }
  }

  @Test
  public void testTpcdsOnQuboleHive() {
    try {
      allTpcds500Queries("qubolehive");
    } catch (Exception e) {
      assertTrue("Exception thrown while executing queries on qubole hive: "
          + e.getMessage(), false);
    }
  }

  @Test
  public void testBasicTypesOnQuboleHive() {
    try {
      basicTypesQuery("qubolehive");
    } catch (Exception e) {
      assertTrue("Exception thrown while executing queries on qubole hive: "
          + e.getMessage(), false);
    }
  }

  @Ignore
  @Test
  public void testBasicTypesOnEMRHive() {
    try {
      basicTypesQuery("hive");
    } catch (Exception e) {
      assertTrue("Exception thrown while executing queries on EMR hive: "
          + e.getMessage(), false);
    }
  }
}
