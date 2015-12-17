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

package com.qubole.quark.plugins.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 4/18/15.
 */
public class H2QueryTest {
  // TODO : setup test-env for redshift OR mock redshift getTable calls
  protected static Connection calciteConnection = null;
  protected static Properties info = null;
  private static final Logger log = LoggerFactory.getLogger(H2QueryTest.class);
  private static final List<String> defaultSchemas = new ArrayList<String>() {{
    add("PUBLIC");
  }};
  public static final String h2Url = "jdbc:h2:mem:H2QueryTest;DB_CLOSE_DELAY=-1";
  public static Connection connection;

  public static void createTables() throws ClassNotFoundException, SQLException {
      Class.forName("org.h2.Driver");
      Properties props = new Properties();
      props.setProperty("user", "sa");
      props.setProperty("password", "");
      connection = DriverManager.getConnection(h2Url, props);

      Statement stmt = connection.createStatement();
      stmt.execute("CREATE TABLE CITIES(i int, j varchar)");
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    createTables();
/*    DriverConnection h2DriverConnection = new DriverConnection(
        DriverType.H2,
        "jdbc:h2:mem:H2QueryTest", "", "sa", "");
    //TODO : check as to why is this needed
    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    calciteConnection =
        DriverManager.getConnection("jdbc:quark:", h2DriverConnection.getProperties());*/
  }

  @AfterClass
  public static void tearDownClass() throws SQLException {
    connection.close();
  }

//  @Test
  public void testGetTables() throws SQLException {
    ResultSet tables =
        calciteConnection.getMetaData().getTables(null, null, null, null);
    List<String> tableList = new ArrayList<>();
    while(tables.next()) {
      tableList.add(tables.getString(3));
    }

    assertThat(tableList).contains("CITIES");
    tables.close();
  }
}
