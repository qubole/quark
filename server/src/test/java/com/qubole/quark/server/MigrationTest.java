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

package com.qubole.quark.server;

import com.qubole.quark.jdbc.ThinClientUtil;

import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by adeshr on 3/7/16.
 */
public class MigrationTest {

  public static Main main;
  public static String dbUrl = "jdbc:h2:mem:MigrationTest;DB_CLOSE_DELAY=-1";

  @BeforeClass
  public static void setUp() {
    String[] args = new String[1];
    args[0] = JsonEndToEndTest.class.getResource("/migrationTest.json").getPath();
    main = new Main(args);

    new Thread(main).start();
  }

  @Ignore
  public void testMigration() throws SQLException, ClassNotFoundException, InterruptedException,
      URISyntaxException, IOException {

    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    Class.forName("org.h2.Driver");
    Connection connection = DriverManager.getConnection(dbUrl, "sa", "");

    Statement stmt = connection.createStatement();
    try {
      stmt.executeQuery("select count(*) from \"schema_version\"");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Table \"schema_version\" not found"));
      stmt.close();
    }

    for (int i=0; i<2; i++) {
      try {
        connection = DriverManager.getConnection(ThinClientUtil.getConnectionUrl("0.0.0.0", 8765), new Properties());
      } catch (RuntimeException e) {
        if (e.getMessage().contains("Connection refused")) {
          Thread.sleep(2000);
        } else {
          throw new RuntimeException(e);
        }
      }
    }

    connection = DriverManager.getConnection(dbUrl, "sa", "");
    stmt = connection.createStatement();
    ResultSet version = stmt.executeQuery("select count(*) from \"schema_version\"");
    version.next();
    assertThat(version.getInt(1), equalTo(12));
  }
}
