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

package com.qubole.quark.fatjdbc.test;

import com.qubole.quark.catalog.db.encryption.AESEncrypt;
import org.flywaydb.core.Flyway;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by adeshr on 3/11/16.
 */
public class DDLViewTest {
  private static final Logger log = LoggerFactory.getLogger(DDLMetaDataTest.class);

  private static final String dbSchemaUrl = "jdbc:h2:mem:DDLViewTest1;DB_CLOSE_DELAY=-1";
  private static final String tpcdsUrl = "jdbc:h2:mem:DDLViewTest2;DB_CLOSE_DELAY=-1";
  private static final String tpcdsViewUrl = "jdbc:h2:mem:DDLViewTest3;DB_CLOSE_DELAY=-1";
  protected static Properties props;
  static {
    props = new Properties();
    props.put("url", dbSchemaUrl);
    props.put("user", "sa");
    props.put("password", "");
    props.put("encryptionKey", "xyz");
  }

  public static void setupTables(String dbUrl, String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {

    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(dbUrl, props);

    Statement stmt = connection.createStatement();
    java.net.URL url = DDLViewTest.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {

    setupTables(tpcdsUrl, "tpcds.sql");
    setupTables(tpcdsViewUrl, "tpcds_views.sql");
    // Inserting test-data in views
    String partitionData = "insert into warehouse_partition (w_warehouse_sk, w_warehouse_id, " +
        "w_warehouse_name,w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, " +
        "w_suite_number, w_city, w_county, w_state, w_zip, w_country,w_gmt_offset) values (1, 'ID', " +
        "'NAME', 400, '200', 'CHURCH STREET', 'BORAD', 'W3K', 'NEW YORK', 'US', 'IO', '333031', 'USA', 12);";

    partitionData = partitionData + " insert into customer_address_partition (ca_address_sk, " +
        "ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city," +
        " ca_country, ca_state, ca_zip, ca_gmt_offset, ca_location_type) values ( 1, '12345', '24', " +
        "'commercialstreet', 'Broadway', '1A', 'NY', 'USA', 'NY', '333031', 10, 'UNKNOWN');";

    partitionData = partitionData + "insert into web_site_partition (web_site_sk, web_rec_start_date," +
        " web_county, web_tax_percentage) values (1, '2015-06-29', 'USA', 12);";

    Connection dbConnection = DriverManager.getConnection(tpcdsUrl, "sa", "");
    dbConnection = DriverManager.getConnection(tpcdsViewUrl, "sa", "");
    dbConnection.createStatement().executeUpdate(partitionData);
    dbConnection.close();

    Flyway flyway = new Flyway();
    flyway.setDataSource(dbSchemaUrl, "sa", "");
    flyway.migrate();

    Properties connInfo = new Properties();
    connInfo.setProperty("url", dbSchemaUrl);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");

    dbConnection = DriverManager.getConnection(dbSchemaUrl, connInfo);

    Statement stmt = dbConnection.createStatement();
    // Sql statement default data-source and view datasource
    String sql = "insert into data_sources(name, type, url, ds_set_id, datasource_type) values "
        + "('CANONICAL', 'H2', '" + tpcdsUrl + "', 1, 'JDBC'); insert into jdbc_sources (id, "
        + "username, password) values(1, 'sa', '');"
        + "update ds_sets set default_datasource_id = 1 where id = 1;";

    sql = sql + "insert into data_sources(name, type, url, ds_set_id, datasource_type) values "
        + "('VIEWS', 'H2', '" + tpcdsViewUrl + "', 1, 'JDBC'); "
        + "insert into jdbc_sources (id, username, password) values(2, 'sa', '');";

    // Sql statement to add views, to be used for drop and alter view respectively.
    sql = sql + " insert into partitions(name, description, cost, query, ds_set_id, destination_id, "
        + "schema_name, table_name) values('web_site_part', 'Web Site Partitionn', 0, "
        + "'select web_site_sk, web_rec_start_date, web_county, web_tax_percentage from canonical.public.web_site where web_name = ''Quark''', "
        + "1, 2, 'PUBLIC', 'WEB_SITE_PARTITION'); ";

    sql = sql + " insert into partitions(name, description, cost, query, ds_set_id, destination_id, "
        + "schema_name, table_name) values('customer_address_part', 'Customer Address Partition', 0,"
        + "'select * from canonical.public.customer_address as c where c.ca_street_name=''commercialstreet''', "
        + "1, 2, 'PUBLIC', 'CUSTOMER_ADDRESS_PARTITION');";

    stmt.execute(sql);
    stmt.close();
  }

  // Helper function to return the number of rows for a query
  private int getSize(String sqlQuery) throws  SQLException {
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);
    ResultSet resultSet = connection.createStatement().executeQuery(sqlQuery);
    assertThat(resultSet.next()).isEqualTo(true);
    connection.close();
    return resultSet.getInt("count(*)");
  }

  @Test
  public void testCreateView() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    String sqlQuery = "select count(*) from warehouse as wr where wr.w_warehouse_sq_ft > 100";
    assertThat(getSize(sqlQuery)).isEqualTo(0);

    String sql1 = "CREATE VIEW warehouse_part set description = \"Warehouse Partition\", cost = 0, "
        + "query=\"select * from canonical.public.warehouse as wr where wr.w_warehouse_sq_ft > 100\", "
        + "destination_id=2, schema_name=\"PUBLIC\", table_name = \"WAREHOUSE_PARTITION\"";
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);
    connection.createStatement().executeUpdate(sql1);
    connection.close();

    assertThat(getSize(sqlQuery)).isEqualTo(1);
  }

  @Test
  public void testAlterView() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    String countQuery = "select count(*) from customer_address as c where c.ca_street_name='commercialstreet'";
    assertThat(getSize(countQuery)).isEqualTo(1);

    String sql1 = "ALTER VIEW customer_address_part set query = \"select * from canonical.public.customer_address as c "
      + "where c.ca_street_name='noncommercialstreet'\"";
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);
    connection.createStatement().executeUpdate(sql1);
    connection.close();

    assertThat(getSize(countQuery)).isEqualTo(0);
  }

  @Test
  public void testNonExistentDrop() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    String sql1 = "DROP VIEW bogus";
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);
    connection.createStatement().executeUpdate(sql1);
    connection.close();
  }

  @Test
  public void testDropView() throws  SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    String countQuery = "select count(*) from canonical.public.web_site where web_name = 'Quark'";
    assertThat(getSize(countQuery)).isEqualTo(1);

    String sql1 = "DROP VIEW web_site_part";
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);
    connection.createStatement().executeUpdate(sql1);
    connection.close();

    assertThat(getSize(countQuery)).isEqualTo(0);
  }

  @Test
  public void testShowViewDDL() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);

    ResultSet rs = connection.createStatement().executeQuery("SHOW VIEW");
    assertThat(rs.next()).isEqualTo(true);
  }

  @Test
  public void testShowViewLike() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    Connection connection = DriverManager.getConnection("jdbc:quark:fat:db:", props);

    ResultSet rs = connection.createStatement().executeQuery("SHOW VIEW LIKE 'warehouse_part'");
    assertThat(rs.next()).isEqualTo(true);
  }
}
