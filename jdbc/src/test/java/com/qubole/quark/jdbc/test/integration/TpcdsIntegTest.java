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

package com.qubole.quark.jdbc.test.integration;

import com.qubole.quark.jdbc.test.integration.utility.IntegTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by amargoor on 11/7/15.
 */
public class TpcdsIntegTest extends IntegTest {

  public static final String h2Url = "jdbc:h2:mem:TpcdsTest;DB_CLOSE_DELAY=-1";
  public static final String cubeUrl = "jdbc:h2:mem:TpcdsCubes;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:TpcdsViews;DB_CLOSE_DELAY=-1";

  public static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException,
      IOException, URISyntaxException {
    setupTables(h2Url, "tpcds.sql");
    setupTables(cubeUrl, "tpcds_cubes.sql");
    setupTables(viewUrl, "tpcds_views.sql");

    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    conn = DriverManager.getConnection("jdbc:quark:"
        + TpcdsIntegTest.class.getResource("/TpcdsModel.json").getPath(), new Properties());
  }

  @Test
  public void simpleQuery() throws SQLException, ClassNotFoundException {

    String query = "select * from canonical.public.web_returns";
    ResultSet resultSet = conn.createStatement().executeQuery(query);

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

  @Test
  public void simpleCubeQuery() throws SQLException, ClassNotFoundException {
    String query = "select sum(wr_net_loss), dd.d_year "
        + "from canonical.public.web_returns as w "
        + "join canonical.public.date_dim as dd on w.wr_returned_date_sk = dd.d_date_sk "
        + "where dd.d_moy > 6 group by dd.d_year";
        //+ "group by dd.d_year";

    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "SELECT SUM(\"TOTAL_NET_LOSS\"), \"D_YEAR\" " +
        "FROM \"PUBLIC\".\"WEB_RETURNS_CUBE\" " +
        "WHERE \"D_MOY\" > 6 AND \"GROUPING__ID\" = '2' " +
        "GROUP BY \"D_YEAR\"";

    ResultSet rs2 = executeQuery(cubeUrl, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

}
