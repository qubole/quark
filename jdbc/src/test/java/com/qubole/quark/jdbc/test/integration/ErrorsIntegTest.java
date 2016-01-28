package com.qubole.quark.jdbc.test.integration;

import com.qubole.quark.jdbc.test.integration.utility.IntegTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by rajatv on 1/28/16.
 */
public class ErrorsIntegTest extends IntegTest {
  public static final String h2Url = "jdbc:h2:mem:SimpleTest;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:SimpleViews;DB_CLOSE_DELAY=-1";
  public static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException,
      IOException, URISyntaxException {
    setupTables(h2Url, "tpcds.sql");
    setupTables(viewUrl, "tpcds_views.sql");

    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    conn = DriverManager.getConnection("jdbc:quark:"
        + TpcdsIntegTest.class.getResource("/SimpleModel.json").getPath(), new Properties());
  }

  @Test
  public void testSyntaxError() throws SQLException, ClassNotFoundException {
    String query = "select count(*) canonical.public.web_returns";
    assertThatThrownBy(() -> conn.createStatement().executeQuery(query)).isInstanceOf(SQLException.class)
        .hasMessageContaining("Encountered \".\" at line 1, column 26.");
  }

  @Test
  public void testSemanticError() throws SQLException, ClassNotFoundException {
    String query = "select count(*) from canonical.public.web_rturns";
    assertThatThrownBy(() -> conn.createStatement().executeQuery(query)).isInstanceOf(SQLException.class)
        .hasMessageContaining("Table 'CANONICAL.PUBLIC.WEB_RTURNS' not found");
  }
}
