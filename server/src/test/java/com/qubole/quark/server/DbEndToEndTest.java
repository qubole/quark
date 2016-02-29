package com.qubole.quark.server;

import org.flywaydb.core.Flyway;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

/**
 * Created by adeshr on 3/3/16.
 */
public class DbEndToEndTest extends EndToEndTest {

  public static String dbUrl = "jdbc:h2:mem:DbTpcds;DB_CLOSE_DELAY=-1";
  static {
    String[] args = new String [1];
    args[0] = JsonEndToEndTest.class.getResource("/dbCatalog.json").getPath();
    main = new Main(args);
    h2Url = "jdbc:h2:mem:DbServerTpcdsTest;DB_CLOSE_DELAY=-1";
    cubeUrl = "jdbc:h2:mem:DbServerTpcdsCubes;DB_CLOSE_DELAY=-1";
    viewUrl = "jdbc:h2:mem:DbServerTpcdsViews;DB_CLOSE_DELAY=-1";
  }

  @BeforeClass
  public static void setUp() throws SQLException, IOException, URISyntaxException,
      ClassNotFoundException {

    new Thread(main).start();

    Flyway flyway = new Flyway();
    flyway.setDataSource(dbUrl, "sa", "");
    flyway.migrate();

    setupTables(dbUrl, "tpcds_db.sql");
    setupTables(h2Url, "tpcds.sql");
    setupTables(cubeUrl, "tpcds_cubes.sql");
    setupTables(viewUrl, "tpcds_views.sql");
  }
}
