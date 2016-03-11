package com.qubole.quark.fatjdbc.test.integration;

import com.qubole.quark.fatjdbc.test.integration.utility.IntegTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Created by rajatv on 1/28/16.
 */
public class ErrorsIntegTest extends IntegTest {
  private static final Logger log = LoggerFactory.getLogger(ErrorsIntegTest.class);

  public static final String h2Url = "jdbc:h2:mem:ErrorTest;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:ErrorViews;DB_CLOSE_DELAY=-1";
  public static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException,
      IOException, URISyntaxException {
    setupTables(h2Url, "tpcds.sql");
    setupTables(viewUrl, "tpcds_views.sql");

    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    conn = DriverManager.getConnection("jdbc:quark:fat:json:"
        + TpcdsIntegTest.class.getResource("/ErrorsModel.json").getPath(), new Properties());
  }

  @Test
  public void testSyntaxError() throws SQLException, ClassNotFoundException {
    String query = "select count(*) canonical.public.web_returns";
    try {
      conn.createStatement().executeQuery(query);
      failBecauseExceptionWasNotThrown(SQLException.class);
    } catch (SQLException e) {
      assertThat((Throwable) e).hasMessageContaining("Encountered \".\" at line 1, column 26.");
    }
  }

  @Test
  public void testSemanticError() throws SQLException, ClassNotFoundException {
    String query = "select count(*) from canonical.public.web_rturns";
    try {
      conn.createStatement().executeQuery(query);
      failBecauseExceptionWasNotThrown(SQLException.class);
    } catch (SQLException e) {
      log.info(e.getMessage());
      assertThat((Throwable) e).hasMessageContaining("Table 'CANONICAL.PUBLIC.WEB_RTURNS' not found");
    }
  }
}
