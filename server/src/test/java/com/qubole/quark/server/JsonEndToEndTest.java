package com.qubole.quark.server;

import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

/**
 * Created by adeshr on 3/3/16.
 */
public class JsonEndToEndTest extends EndToEndTest {

  static {
    String[] args = new String [1];
    args[0] = JsonEndToEndTest.class.getResource("/jsonCatalog.json").getPath();
    main = new Main(args);
    h2Url = "jdbc:h2:mem:JsonTpcdsTest;DB_CLOSE_DELAY=-1";
    cubeUrl = "jdbc:h2:mem:JsonTpcdsCubes;DB_CLOSE_DELAY=-1";
    viewUrl = "jdbc:h2:mem:JsonTpcdsViews;DB_CLOSE_DELAY=-1";
  }

  @BeforeClass
  public static void setUp() throws SQLException, IOException, URISyntaxException,
      ClassNotFoundException {

    new Thread(main).start();

    setupTables(h2Url, "tpcds.sql");
    setupTables(cubeUrl, "tpcds_cubes.sql");
    setupTables(viewUrl, "tpcds_views.sql");
  }
}
