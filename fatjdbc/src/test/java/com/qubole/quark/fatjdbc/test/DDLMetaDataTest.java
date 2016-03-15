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

import com.qubole.quark.catalog.db.encryption.MysqlAES;
import com.qubole.quark.fatjdbc.test.utility.MetaDataTest;
import org.flywaydb.core.Flyway;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by amoghm on 2/18/16.
 */
public class DDLMetaDataTest {
  private static final Logger log = LoggerFactory.getLogger(DDLMetaDataTest.class);

  private static final String dbSchemaUrl = "jdbc:h2:mem:DDLMetaDataTest;DB_CLOSE_DELAY=-1";
  private static final String inputUrl = "jdbc:h2:mem:DDLMetaDataTest1;DB_CLOSE_DELAY=-1";
  protected static String h2Url;
  protected static Properties props;
  static {
    h2Url = "jdbc:h2:mem:MetaDataTest3;DB_CLOSE_DELAY=-1";
    props = new Properties();
    props.put("url", dbSchemaUrl);
    props.put("user", "sa");
    props.put("password", "");
    props.put("encryptionKey", "xyz");
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    MetaDataTest.setUpClass(h2Url);
    MetaDataTest.setUpClass(inputUrl);
    Flyway flyway = new Flyway();
    flyway.setDataSource(dbSchemaUrl, "sa", "");
    flyway.migrate();

    // Encrypting url, username and password before storing in db
    MysqlAES mysqlAES = MysqlAES.getInstance();
    mysqlAES.setKey("xyz");
    String url = mysqlAES.convertToDatabaseColumn(h2Url);
    String username = mysqlAES.convertToDatabaseColumn("sa");
    String password = mysqlAES.convertToDatabaseColumn("");

    Properties connInfo = new Properties();
    connInfo.setProperty("url", dbSchemaUrl);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");

    final Connection dbConnection = DriverManager.getConnection(dbSchemaUrl, connInfo);

    Statement stmt = dbConnection.createStatement();
    String sql = "insert into data_sources(name, type, url, ds_set_id, datasource_type) values "
        + "('H2', 'H2', '" + url + "', 1, 'JDBC'); insert into jdbc_sources (id, "
        + "username, password) values(1, '" + username + "', '" + password + "');"
        + "update ds_sets set default_datasource_id = 1 where id = 1;";

    stmt.execute(sql);
    stmt.close();
  }

  private void getSchema(Connection connection, List<String> catalogList, List<String> schemaList) throws SQLException {
    ResultSet schemas =
        connection.getMetaData().getSchemas();
    while (schemas.next()) {
      catalogList.add(schemas.getString("TABLE_CATALOG"));
      schemaList.add(schemas.getString("TABLE_SCHEM"));
    }

    schemas.close();
  }

  @Test
  public void testCreateJdBc() throws SQLException {
    String sql = "CREATE DATASOURCE (name, type, url, ds_set_id, username, datasource_type)" +
        " values(\"H2_new\", \"H2\", \"" + inputUrl + "\", 1, \"sa\", \"JDBC\")";
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:db:", props);
    List<String> catalogList1 = new ArrayList<>();
    List<String> schemaList1 = new ArrayList<>();
    getSchema(connection, catalogList1, schemaList1);
    connection.createStatement().executeUpdate(sql);

    List<String> catalogList2 = new ArrayList<>();
    List<String> schemaList2 = new ArrayList<>();
    getSchema(connection, catalogList2, schemaList2);
    connection.close();

    assertThat(schemaList2).contains("PUBLIC");
    assertThat(schemaList2.size())
        .isEqualTo(schemaList1.size() + 1);

    assertThat(catalogList2).contains("H2_new");
    assertThat(catalogList2.size())
        .isEqualTo(catalogList1.size() + 1);
  }

  @Test(expected = SQLException.class)
  public void testCreateWrongParam() throws SQLException {
    String sql = "CREATE DATASOURCE (name, type, url, ds_set_id, username, datasource_type)" +
        " values(\"H2_new\", \"H2\", \"" + inputUrl + "\", \"1\", \"sa\", \"JDBC\")";
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:db:", props);
    List<String> catalogList1 = new ArrayList<>();
    List<String> schemaList1 = new ArrayList<>();
    getSchema(connection, catalogList1, schemaList1);
    connection.createStatement().executeUpdate(sql);

    List<String> catalogList2 = new ArrayList<>();
    List<String> schemaList2 = new ArrayList<>();
    getSchema(connection, catalogList2, schemaList2);
    connection.close();

    assertThat(schemaList2).contains("PUBLIC");
    assertThat(schemaList2.size())
        .isEqualTo(schemaList1.size() + 1);

    assertThat(catalogList2).contains("H2_new");
    assertThat(catalogList2.size())
        .isEqualTo(catalogList1.size() + 1);
  }

  @Test
  public void testAlterJdBc() throws SQLException {
    String sql = "ALTER DATASOURCE SET name = \"H2_test\" where id = 1";
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:db:", props);
    connection.createStatement().executeUpdate(sql);
    List<String> catalogList = new ArrayList<>();
    List<String> schemaList = new ArrayList<>();
    getSchema(connection, catalogList, schemaList);
    connection.close();

    assertThat(schemaList).contains("PUBLIC");
    assertThat(catalogList).contains("H2_test");
  }

  @Test
  public void testDropJdbc() throws SQLException {
    String sql1 = "CREATE DATASOURCE (name, type, url, ds_set_id, username, datasource_type)" +
        " values(\"H2_2\", \"H2\", \"" + inputUrl + "\", 1, \"sa\", \"JDBC\")";
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:db:", props);
    List<String> catalogList1 = new ArrayList<>();
    List<String> schemaList1 = new ArrayList<>();
    getSchema(connection, catalogList1, schemaList1);
    connection.createStatement().executeUpdate(sql1);

    List<String> catalogList2 = new ArrayList<>();
    List<String> schemaList2 = new ArrayList<>();
    getSchema(connection, catalogList2, schemaList2);

    assertThat(schemaList2.size())
        .isEqualTo(schemaList1.size() + 1);
    assertThat(catalogList2.size())
        .isEqualTo(catalogList1.size() + 1);

    String sql2 = "DROP DATASOURCE where id = 2";
    connection.createStatement().executeUpdate(sql2);

    List<String> catalogList3 = new ArrayList<>();
    List<String> schemaList3 = new ArrayList<>();
    getSchema(connection, catalogList3, schemaList3);
    connection.close();

    assertThat(schemaList3.size())
        .isEqualTo(schemaList2.size() - 1);
    assertThat(catalogList3.size())
        .isEqualTo(catalogList2.size() - 1);
  }

  @Test(expected = SQLException.class)
  public void testAlterJdBcWrongParam() throws SQLException {
    String sql = "ALTER DATASOURCE SET name = \"H2_test\" where name = 'H2'";
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:", props);
    connection.createStatement().executeUpdate(sql);
    List<String> catalogList = new ArrayList<>();
    List<String> schemaList = new ArrayList<>();
    getSchema(connection, catalogList, schemaList);
    connection.close();

    assertThat(schemaList).contains("PUBLIC");
    assertThat(catalogList).contains("H2_test");
  }

  @Test
  public void testAlterNonExistentSource() throws SQLException {
    String sql = "ALTER DATASOURCE SET name = \"H2_NonExist\" where id = 10";
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:db:", props);
    connection.createStatement().executeUpdate(sql);
    List<String> catalogList = new ArrayList<>();
    List<String> schemaList = new ArrayList<>();
    getSchema(connection, catalogList, schemaList);
    connection.close();
    assertThat(catalogList).doesNotContain("H2_NonExist");
  }

  @Test
  public void testDropNonExistentJdbc() throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:db:", props);
    List<String> catalogList1 = new ArrayList<>();
    List<String> schemaList1 = new ArrayList<>();
    getSchema(connection, catalogList1, schemaList1);

    String sql1 = "DROP DATASOURCE where id = 10";
    connection.createStatement().executeUpdate(sql1);

    List<String> catalogList2 = new ArrayList<>();
    List<String> schemaList2 = new ArrayList<>();
    getSchema(connection, catalogList2, schemaList2);
    connection.close();

    assertThat(schemaList2.size())
        .isEqualTo(schemaList1.size());
    assertThat(catalogList2.size())
        .isEqualTo(catalogList1.size());
  }
}
