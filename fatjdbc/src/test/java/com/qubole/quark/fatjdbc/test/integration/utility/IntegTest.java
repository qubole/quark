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

package com.qubole.quark.fatjdbc.test.integration.utility;

import com.qubole.quark.fatjdbc.test.integration.TpcdsIntegTest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by dev on 11/16/15.
 */
public abstract class IntegTest {

  public static void setupTables(String dbUrl, String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(dbUrl, props);

    Statement stmt = connection.createStatement();
    java.net.URL url = TpcdsIntegTest.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
  }

  public static ResultSet executeQuery(String url, String query)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(url, props);
    Statement stmt = connection.createStatement();
    return stmt.executeQuery(query);
  }

  public static ResultSet prepareAndExecuteQuery(String url, String query)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(url, props);
    return connection.prepareStatement(query).executeQuery();
  }

  public static boolean execute(String url, String query)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(url, props);
    Statement stmt = connection.createStatement();
    return stmt.execute(query);
  }

  public static int executeUpdate(String url, String query)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(url, props);
    Statement stmt = connection.createStatement();
    return stmt.executeUpdate(query);
  }

  public static void checkSameResultSet(ResultSet rs1, ResultSet rs2)
      throws SQLException {
    boolean columnCheckDone = false;
    int columnCount = 0;
    while (rs1.next() && rs2.next()) {
      if (!columnCheckDone) {
        columnCount = rs1.getMetaData().getColumnCount();
        assertThat(columnCount,
            equalTo(rs2.getMetaData().getColumnCount()));
        columnCheckDone = true;
      }

      for(int i = 1; i <= columnCount; i++) {
        assertThat(rs1.getObject(i), equalTo(rs2.getObject(i)));
      }
    }

    assertThat(rs1.next(), equalTo(rs2.next()));
  }
}
