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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 10/29/15.
 */
public abstract class SelectTest {
  private static final Logger log = LoggerFactory.getLogger(SelectTest.class);

  private static Connection h2Connection;
  protected static String dbUrl = "jdbc:h2:mem:SelectTest;DB_CLOSE_DELAY=-1";
  protected static Properties props;

  protected abstract String getConnectionUrl();

  public static void setUpClass(String dbUrl) throws Exception {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    Class.forName("org.h2.Driver");

    Properties connInfo = new Properties();
    connInfo.setProperty("url", dbUrl);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");

    h2Connection = DriverManager.getConnection(dbUrl, connInfo);

    Statement stmt = h2Connection.createStatement();
    String sql = "create table simple (i int, j int);"
        + "insert into simple values(1, 4);"
        + "insert into simple values(2, 5);"
        + "insert into simple values(3, 6);";

    stmt.execute(sql);
    stmt.close();
  }

  @AfterClass
  public static void tearDownClass() throws SQLException {
    h2Connection.close();
  }

  @Test
  public void testSimpleSelect() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    Connection connection =
        DriverManager.getConnection(getConnectionUrl(), props);

    Statement statement = connection.createStatement();
    ResultSet rows =
        statement.executeQuery("select * from simple");

    ArrayList<Integer> firstColumn = new ArrayList<>();
    ArrayList<Integer> secondColumn = new ArrayList<>();

    assertThat(rows.getMetaData().getColumnCount()).isEqualTo(2);
    int totalRows = 0;
    while (rows.next()) {
      totalRows++;
      firstColumn.add(rows.getInt(1));
      secondColumn.add(rows.getInt(2));
    }
    statement.close();
    connection.close();

    assertThat(firstColumn).contains(1, 2, 3);
    assertThat(secondColumn).contains(4, 5, 6);
    assertThat(totalRows).isEqualTo(3);

  }
}
