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

import com.qubole.quark.jdbc.QuarkDriver;
import com.qubole.quark.jdbc.ThinClientUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by adeshr on 2/24/16.
 */
public abstract class EndToEndTest {
  protected static final Log LOG = LogFactory.getLog(Main.class);

  public static Main main;
  public static String h2Url;
  public static String cubeUrl;
  public static String viewUrl;

  public static void setupTables(String dbUrl, String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {

    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(dbUrl, props);

    Statement stmt = connection.createStatement();
    java.net.URL url = EndToEndTest.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
  }

  @AfterClass
  public static void afterClass() {
    if (main.getServer() != null) {
      main.getServer().stop();
    }
  }

  @Test
  public void testClient() throws SQLException, ClassNotFoundException, InterruptedException {
    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    Class.forName("org.h2.Driver");
    Connection connection = null;

    // Due to threading, server might not be up.
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

    String query = "select * from canonical.public.web_returns";
    ResultSet resultSet = connection.createStatement().executeQuery(query);

    List<String> wrItemSk = new ArrayList<String>();
    List<String> wrOrderNumber = new ArrayList<String>();
    while (resultSet.next()) {
      wrItemSk.add(resultSet.getString("wr_item_sk"));
      wrOrderNumber.add(resultSet.getString("wr_order_number"));
    }

    assertThat(wrItemSk.size(), equalTo(1));
    assertThat(wrOrderNumber.size(), equalTo(1));
    assertThat(wrItemSk.get(0), equalTo("1"));
    assertThat(wrOrderNumber.get(0), equalTo("10"));
  }
}
