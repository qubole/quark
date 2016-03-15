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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by adeshr on 2/18/16.
 */
public class DbMetaDataTest extends MetaDataTest {
  private static final Logger log = LoggerFactory.getLogger(DbMetaDataTest.class);

  private static final String dbSchemaUrl = "jdbc:h2:mem:DbMetaDataTest;DB_CLOSE_DELAY=-1";
  private static Connection dbConnection;
  static {
    h2Url = "jdbc:h2:mem:MetaDataTest2;DB_CLOSE_DELAY=-1";
    props = new Properties();
    props.put("url", dbSchemaUrl);
    props.put("user", "sa");
    props.put("password", "");
    props.put("encryptionKey", "easy");
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    MetaDataTest.setUpClass(h2Url);
    Flyway flyway = new Flyway();
    flyway.setDataSource(dbSchemaUrl, "sa", "");
    flyway.migrate();

    // Encrypting url, username and password before storing in db
    MysqlAES mysqlAES = MysqlAES.getInstance();
    mysqlAES.setKey("easy");
    String url = mysqlAES.convertToDatabaseColumn(h2Url);
    String username = mysqlAES.convertToDatabaseColumn("sa");
    String password = mysqlAES.convertToDatabaseColumn("");

    Properties connInfo = new Properties();
    connInfo.setProperty("url", dbSchemaUrl);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");

    dbConnection = DriverManager.getConnection(dbSchemaUrl, connInfo);

    Statement stmt = dbConnection.createStatement();
    String sql = "insert into data_sources(name, type, url, ds_set_id, datasource_type) values "
        + "('H2', 'H2', '" + url + "', 1, 'JDBC'); insert into jdbc_sources (id, "
        + "username, password) values(1, '" + username + "', '" + password + "');" +
        "update ds_sets set default_datasource_id = 1 where id = 1;";

    stmt.execute(sql);
    stmt.close();
  }

  protected String getConnectionUrl() {
    return "jdbc:quark:fat:db:";
  }
}
