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

package com.qubole.quark.fatjdbc.test.integration;

import com.qubole.quark.fatjdbc.test.integration.utility.IntegTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by amoghm on 11/16/15.
 */
public class SimpleIntegTest extends IntegTest {
  public static final String h2Url = "jdbc:h2:mem:SimpleTest;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:SimpleViews;DB_CLOSE_DELAY=-1";
  public static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException,
      IOException, URISyntaxException {
    setupTables(h2Url, "tpcds.sql");
    setupTables(viewUrl, "tpcds_views.sql");

    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    conn = DriverManager.getConnection("jdbc:quark:fat:json:"
        + TpcdsIntegTest.class.getResource("/SimpleModel.json").getPath(), new Properties());
  }

  @Test
  public void simpleAggregateCountQuery() throws SQLException, ClassNotFoundException {

    String query = "select count(*) from canonical.public.web_returns";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "select count(*) from public.web_returns";

    ResultSet rs2 = executeQuery(h2Url, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Test
  public void simpleAggregateSumQuery() throws SQLException, ClassNotFoundException {

    String query = "select sum(WR_RETURN_AMT) from canonical.public.web_returns";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "select sum(WR_RETURN_AMT) from public.web_returns";

    ResultSet rs2 = executeQuery(h2Url, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Test
  public void simpleQueryOnDefaultWithNamespace()
      throws SQLException, ClassNotFoundException {

    String query = "select * from canonical.public.web_returns";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "select * from public.web_returns";

    ResultSet rs2 = executeQuery(h2Url, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Ignore("Need to enable direct accessing schemas of default data source without namespace")
  @Test
  public void simpleQueryOnDefaultWithoutNamespace()
      throws SQLException, ClassNotFoundException {

    String query = "select * from public.web_returns";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "select * from public.web_returns";

    ResultSet rs2 = executeQuery(h2Url, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Test
  public void simpleQueryOnDefaultWithoutNamespaceAndDefaultSchema()
      throws SQLException, ClassNotFoundException {

    String query = "select * from web_returns";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "select * from public.web_returns";

    ResultSet rs2 = executeQuery(h2Url, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Test
  public void simpleQueryOnNonDefaultQuery()
      throws SQLException, ClassNotFoundException {

    String query = "select * from views.public.web_site_partition";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "select * from public.web_site_partition";
    ResultSet rs2 = executeQuery(viewUrl, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Ignore
  @Test
  public void simplePrepareQueryOnNonDefaultQuery()
      throws SQLException, ClassNotFoundException {

    String query = "select * from views.public.web_site_partition";
    conn.prepareStatement(query).executeQuery();
  }

  @Ignore
  @Test
  public void simpleExplainQueryOnNonDefaultQuery()
      throws SQLException, ClassNotFoundException {

    String query =
        "explain plan for select * from views.public.web_site_partition";
    ResultSet rs1 = conn.createStatement().executeQuery(query);

    String parsedQuery = "explain plan for select * from public.web_site_partition";
    ResultSet rs2 = executeQuery(viewUrl, parsedQuery);

    checkSameResultSet(rs1, rs2);
  }

  @Ignore
  @Test
  public void simpleInsertQueryOnNonDefaultQuery()
      throws SQLException, ClassNotFoundException {

    String query =
        "INSERT INTO views.public.web_site_partition "
            + "VALUES(10, CAST('2015-01-01' AS DATE), 'county', 56.7)";
    int updateRes1 = conn.createStatement().executeUpdate(query);

    String parsedQuery = "select * from public.web_site_partition";
    int updateRes2 = executeUpdate(viewUrl, parsedQuery);

    assert (updateRes1 == updateRes2);
  }
}
