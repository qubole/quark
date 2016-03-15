package com.qubole.quark.fatjdbc.test;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 2/9/16.
 */
public class ConnectionTest {
  private static final Logger log = LoggerFactory.getLogger(ConnectionTest.class);

  public static final String h2Url = "jdbc:h2:mem:ConnectionTest;DB_CLOSE_DELAY=-1";
  public static final String cubeUrl = "jdbc:h2:mem:ConnectionCubes;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:ConnectionViews;DB_CLOSE_DELAY=-1";

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException,
      IOException, URISyntaxException {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    setupTables(h2Url, "tpcds.sql");
    setupTables(cubeUrl, "tpcds_cubes.sql");
    setupTables(viewUrl, "tpcds_views.sql");
  }

  private static void setupTables(String dbUrl, String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(dbUrl, props);

    Statement stmt = connection.createStatement();
    java.net.URL url = ConnectionTest.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
    stmt.close();
    connection.close();
  }

  public void testValidImpl(String testName) throws SQLException, URISyntaxException {
    Properties props = new Properties();
    java.net.URL url = ConnectionTest.class.getResource("/" + "ConnectionTest/" +
        testName + ".json");
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:json:" + resPath.toString(), props);

    Statement statement = connection.createStatement();
    ResultSet rows =
        statement.executeQuery("select * from h2.public.warehouse as wr where wr" +
            ".w_warehouse_sq_ft > 200");

    assertThat(rows.getMetaData().getColumnCount()).isEqualTo(14);

    statement.close();
    connection.close();

  }

  @Test
  public void testEmptyDataSources() throws SQLException, URISyntaxException {
    Properties props = new Properties();
    java.net.URL url = ConnectionTest.class.getResource("/" + "ConnectionTest/" +
        "testEmptyDataSources.json");
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    Connection connection =
        DriverManager.getConnection("jdbc:quark:fat:json:" + resPath.toString(), props);
    connection.close();
  }

  @Test
  public void testValid() throws SQLException, URISyntaxException {
    testValidImpl("testValid");
  }

  @Test
  public void testEmptyRelSchema() throws SQLException, URISyntaxException {
    testValidImpl("testEmptyRelSchema");
  }

  @Test
  public void testEmptyCube() throws SQLException, URISyntaxException {
    testValidImpl("testEmptyCube");
  }

  @Test
  public void testEmptyView() throws SQLException, URISyntaxException {
    testValidImpl("testEmptyView");
  }

  @Test
  public void testEmptyGroup() throws SQLException, URISyntaxException {
    testValidImpl("testEmptyGroup");
  }

}
