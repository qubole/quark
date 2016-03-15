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

package com.qubole.quark.fatjdbc.test.utility;

import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 10/28/15.
 */
public abstract class MetaDataTest {
  private static final Logger log = LoggerFactory.getLogger(MetaDataTest.class);

  protected static String h2Url;
  private static Connection h2Connection;
  protected static Properties props;

  protected abstract String getConnectionUrl();

  public static void setUpClass(String h2Url) throws Exception {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    Class.forName("org.h2.Driver");

    Properties connInfo = new Properties();
    connInfo.setProperty("url", h2Url);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");

    h2Connection = DriverManager.getConnection(h2Url, connInfo);

    Statement stmt = h2Connection.createStatement();
    stmt.execute("create table simple(i int, j int)");
    stmt.close();
  }

  @AfterClass
  public static void tearDownClass() throws SQLException {
    h2Connection.close();
  }



  @Test
  public void testDriverRegistered() throws SQLException {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    List<Driver> driverList = Collections.list(drivers);
    List<String> driverNames = new ArrayList<>();
    for (Driver driver : driverList) {
      driverNames.add(driver.getClass().getCanonicalName());
      log.debug("Found driver: " + driver.getClass().getCanonicalName());
    }
    assertThat(driverNames).contains("com.qubole.quark.fatjdbc.QuarkDriver");
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    Connection connection =
        DriverManager.getConnection(getConnectionUrl(), props);

    ResultSet catalogs =
        connection.getMetaData().getCatalogs();
    List<String> catalogList = new ArrayList<>();

    while (catalogs.next()) {
      catalogList.add(catalogs.getString("TABLE_CAT"));
    }

    catalogs.close();
    connection.close();

    assertThat(catalogList).contains("H2");
    assertThat(catalogList).contains("QUARK_METADATA");
    assertThat(catalogList).contains("metadata");
    assertThat(catalogList.size()).isEqualTo(3);
  }

  @Test
  public void testGetSchemas() throws SQLException {
    Connection connection =
        DriverManager.getConnection(getConnectionUrl(), props);

    ResultSet schemas =
        connection.getMetaData().getSchemas();
    List<String> catalogList = new ArrayList<>();
    List<String> schemaList = new ArrayList<>();

    while (schemas.next()) {
      catalogList.add(schemas.getString("TABLE_CATALOG"));
      schemaList.add(schemas.getString("TABLE_SCHEM"));
    }

    schemas.close();
    connection.close();

    assertThat(schemaList).contains("PUBLIC");
    assertThat(schemaList.size()).isEqualTo(1);

    assertThat(catalogList).contains("H2");
    assertThat(catalogList.size()).isEqualTo(1);
  }

  @Test
  public void testGetTables() throws SQLException {
    Connection connection =
        DriverManager.getConnection(getConnectionUrl(), props);

    ResultSet tables =
        connection.getMetaData().getTables(null, null, null, null);
    List<String> tableList = new ArrayList<>();

    while (tables.next()) {
      tableList.add(tables.getString(3));
    }

    tables.close();
    connection.close();

    assertThat(tableList).contains("SIMPLE");
    assertThat(tableList.size()).isEqualTo(1);
    tables.close();
  }

  @Test
  public void testSearchTable() throws SQLException {
    Connection connection =
        DriverManager.getConnection(getConnectionUrl(), props);

    ResultSet tables =
        connection.getMetaData().getTables(null, null, "SIMPLE", null);
    List<String> tableList = new ArrayList<>();

    while (tables.next()) {
      tableList.add(tables.getString(3));
    }

    tables.close();
    connection.close();

    assertThat(tableList).contains("SIMPLE");
    assertThat(tableList.size()).isEqualTo(1);
    tables.close();
  }

  @Test
  public void testGetColumns() throws SQLException {
    Connection connection =
        DriverManager.getConnection(getConnectionUrl(), props);

    ResultSet columns =
        connection.getMetaData().getColumns(null, "PUBLIC", "SIMPLE", null);
    List<String> columnList = new ArrayList<>();

    while (columns.next()) {
      columnList.add(columns.getString("COLUMN_NAME"));
    }

    columns.close();
    connection.close();

    assertThat(columnList).contains("I", "J");
    assertThat(columnList.size()).isEqualTo(2);
  }
}
