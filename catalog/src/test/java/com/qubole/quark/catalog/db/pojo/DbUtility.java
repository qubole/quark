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

package com.qubole.quark.catalog.db.pojo;

import org.flywaydb.core.Flyway;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by adeshr on 2/19/16.
 */
public abstract class DbUtility {

  public static void setUpDb(String dbSchemaUrl,
                             String username,
                             String password,
                             String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(dbSchemaUrl, username, password);
    flyway.migrate();

    setupTables(dbSchemaUrl, username, password, filename);
  }

  private static void setupTables(String dbUrl, String username, String password, String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", username);
    props.setProperty("password", password);

    Connection connection = DriverManager.getConnection(dbUrl, props);

    Statement stmt = connection.createStatement();
    java.net.URL url = DSSetTest.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
  }
}
