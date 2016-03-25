package com.qubole.quark.fatjdbc.test;

import com.qubole.quark.catalog.db.encryption.MysqlAES;
import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.sql.ResultProcessor;
import org.apache.calcite.sql.SqlDialect;
import org.flywaydb.core.Flyway;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by amoghm on 3/21/16.
 */
public class MaterializedViewJoinTest {
  private static Connection h2Connection;
  protected static String dbUrl = "jdbc:h2:mem:MaterializedViewJoinTest;DB_CLOSE_DELAY=-1";

  private static final String dbSchemaUrl = "jdbc:h2:mem:MaterializedViewJoinTest1;DB_CLOSE_DELAY=-1";
  private static Connection dbConnection;
  private static Properties connInfo;

  @BeforeClass
  public static void setUpDb() throws Exception {
    MaterializedViewJoinTest.setUpClass(dbUrl);
    Flyway flyway = new Flyway();
    flyway.setDataSource(dbSchemaUrl, "sa", "");
    flyway.migrate();

    // Encrypting url, username and password before storing in db
    MysqlAES mysqlAES = MysqlAES.getInstance();
    mysqlAES.setKey("key");
    String url = mysqlAES.convertToDatabaseColumn(dbUrl);
    String username = mysqlAES.convertToDatabaseColumn("sa");
    String password = mysqlAES.convertToDatabaseColumn("");

    connInfo = new Properties();
    connInfo.setProperty("url", dbSchemaUrl);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");
    connInfo.setProperty("db:" +
        MaterializedViewJoinTest.class.
            getResource("/HiveUDFModel.json").getPath(), "");
    connInfo.setProperty("schemaFactory",
        "com.qubole.quark.catalog.db.SchemaFactory");
    connInfo.setProperty("encryptionKey", "key");

    dbConnection = DriverManager.getConnection(dbSchemaUrl, connInfo);

    Statement stmt = dbConnection.createStatement();
    String sql = "insert into data_sources(name, type, url, ds_set_id, datasource_type) values "
        + "('H2', 'H2', '" + url + "', 1, 'JDBC'); insert into jdbc_sources (id, "
        + "username, password) values(1, '" + username + "', '" + password + "');"
        + "update ds_sets set default_datasource_id = 1 where id = 1;"
        + "insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`,"
        + " `destination_id`,`schema_name`, `table_name`)"
        + " VALUES('test_hist_part', 'Test History Partition', 0, "
        + "'select * from h2.public.test_hist as h where h.timeout > 300',"
        + " 1, 1, 'PUBLIC', 'TEST_HIST_PARTITION');"
        /*+ "insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`,"
        + " `destination_id`,`schema_name`, `table_name`)"
        + " VALUES('test_hist_part1', 'Test History Partition1', 0, "
        + "'select * from h2.public.test_hist as h where to_date(h.created_at) < \''2015-02-01\''',"
        + " 1, 1, 'PUBLIC', 'TEST_HIST_PARTITION');"
        + "insert into partitions(`name`, `description`, `cost`, `query`, `ds_set_id`,"
        + " `destination_id`,`schema_name`, `table_name`)"
        + " VALUES('test_hist_part12', 'Test History Partition2', 0, "
        + "'select * from h2.public.test_hist as h where to_date(h.created_at) > DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 60)',"
        + " 1, 1, 'PUBLIC', 'TEST_HIST_PARTITION1');"*/;

    stmt.execute(sql);
    stmt.close();
  }

  public static void setUpClass(String dbUrl) throws Exception {
    Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
    Class.forName("org.h2.Driver");

    Properties connInfo = new Properties();
    connInfo.setProperty("url", dbUrl);
    connInfo.setProperty("user", "sa");
    connInfo.setProperty("password", "");

    h2Connection = DriverManager.getConnection(dbUrl, connInfo);

    Statement stmt = h2Connection.createStatement();
    java.net.URL url = MaterializedViewJoinTest.class.getResource("/HiveUDF.sql");
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
    stmt.close();
  }

  @AfterClass
  public static void tearDownClass() throws SQLException {
    h2Connection.close();
  }

  @Test
  public void testOptWithJoin() throws Exception {
    String sql = "select\n" +
        "qh.created_at as dt, \n" +
        "count(qh.id) as num_queries\n" +
        "\n" +
        "from test_hist qh\n" +
        "join uinfo ui\n" +
        "  on qh.qbol_user_id = ui.qu_id\n" +
        "join acc externals\n" +
        "  on externals.id = ui.a_id\n" +
        "\n" +
        "where qh.timeout >= 350\n" +
        "and command_type = 'HiveCommand'\n" +
        "and qlog like '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%'\n" +
        "and customer_name like 'amogh'\n" +
        "\n" +
        "group by \n" +
        "qh.created_at\n" +
        "\n" +
        "order by dt asc";
    SqlQueryParser parser = new SqlQueryParser(connInfo);
    final SqlQueryParser.SqlQueryParserResult result = parser.parse(sql);
    final String hiveQuery = ResultProcessor.getParsedSql(result.getRelNode(),
        SqlDialect.DatabaseProduct.HIVE.getDialect());
    assertEquals("SELECT CREATED_AT, COUNT(ID) NUM_QUERIES FROM " +
        "PUBLIC.TEST_HIST_PARTITION INNER JOIN H2.PUBLIC.UINFO " +
        "ON TEST_HIST_PARTITION.QBOL_USER_ID = UINFO.QU_ID " +
        "INNER JOIN H2.PUBLIC.ACC ON UINFO.A_ID = ACC.ID " +
        "WHERE ACC.CUSTOMER_NAME LIKE 'amogh' AND " +
        "(TEST_HIST_PARTITION.TIMEOUT >= 350 " +
        "AND TEST_HIST_PARTITION.COMMAND_TYPE = 'HiveCommand' " +
        "AND TEST_HIST_PARTITION.QLOG " +
        "LIKE '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%') " +
        "GROUP BY CREATED_AT ORDER BY CREATED_AT", hiveQuery);
  }

  @Test
  public void testNoOptWeakerFilter() throws Exception {
    String sql = "select\n" +
        "qh.created_at as dt, \n" +
        "count(qh.id) as num_queries\n" +
        "\n" +
        "from test_hist qh\n" +
        "join uinfo ui\n" +
        "  on qh.qbol_user_id = ui.qu_id\n" +
        "join acc externals\n" +
        "  on externals.id = ui.a_id\n" +
        "\n" +
        "where qh.timeout >= 200\n" +
        "and command_type = 'HiveCommand'\n" +
        "and qlog like '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%'\n" +
        "and customer_name like 'amogh'\n" +
        "\n" +
        "group by \n" +
        "qh.created_at\n" +
        "\n" +
        "order by dt asc";
    SqlQueryParser parser = new SqlQueryParser(connInfo);
    final SqlQueryParser.SqlQueryParserResult result = parser.parse(sql);
    final String hiveQuery = ResultProcessor.getParsedSql(result.getRelNode(),
        SqlDialect.DatabaseProduct.HIVE.getDialect());
    assertEquals("SELECT CREATED_AT, COUNT(ID) NUM_QUERIES FROM " +
        "H2.PUBLIC.TEST_HIST INNER JOIN H2.PUBLIC.UINFO " +
        "ON TEST_HIST.QBOL_USER_ID = UINFO.QU_ID " +
        "INNER JOIN H2.PUBLIC.ACC ON UINFO.A_ID = ACC.ID " +
        "WHERE ACC.CUSTOMER_NAME LIKE 'amogh' AND " +
        "(TEST_HIST.TIMEOUT >= 200 " +
        "AND TEST_HIST.COMMAND_TYPE = 'HiveCommand' " +
        "AND TEST_HIST.QLOG " +
        "LIKE '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%') " +
        "GROUP BY CREATED_AT ORDER BY CREATED_AT", hiveQuery);
  }
  /*@Test
  public void testMVOptWithJoinAndhiveOp() throws Exception {
    String sql = "select\n" +
        "to_date(qh.created_at) as dt, \n" +
        "count(qh.id) as num_queries\n" +
        "\n" +
        "from test_hist qh\n" +
        "join uinfo ui\n" +
        "  on qh.qbol_user_id = ui.qu_id\n" +
        "join acc externals\n" +
        "  on externals.id = ui.a_id\n" +
        "\n" +
        "where to_date(qh.created_at) >= date_sub(from_unixtime(unix_timestamp()),30)\n" +
        "and command_type = 'HiveCommand'\n" +
        "and qlog like '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%'\n" +
        "and customer_name like 'amogh'\n" +
        "\n" +
        "group by \n" +
        "to_date(qh.created_at)\n" +
        "\n" +
        "order by dt asc";
    SqlQueryParser parser = new SqlQueryParser(connInfo);
    final SqlQueryParser.SqlQueryParserResult result = parser.parse(sql);
    final String hiveQuery = ResultProcessor.getParsedSql(result.getRelNode(),
        SqlDialect.DatabaseProduct.HIVE.getDialect());
    assertEquals("SELECT TO_DATE(TEST_HIST_PARTITION1.CREATED_AT) DT, "
        + "COUNT(TEST_HIST_PARTITION1.ID) NUM_QUERIES "
        + "FROM PUBLIC.TEST_HIST_PARTITION1 INNER JOIN H2.PUBLIC.UINFO "
        + "ON TEST_HIST_PARTITION1.QBOL_USER_ID = UINFO.QU_ID INNER JOIN H2.PUBLIC.ACC "
        + "ON UINFO.A_ID = ACC.ID "
        + "WHERE ACC.CUSTOMER_NAME LIKE 'amogh' AND "
        + "(TO_DATE(TEST_HIST_PARTITION1.CREATED_AT) >= "
        + "DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP()), 30) "
        + "AND TEST_HIST_PARTITION1.COMMAND_TYPE = 'HiveCommand' "
        + "AND TEST_HIST_PARTITION1.QLOG LIKE '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%') "
        + "GROUP BY TO_DATE(TEST_HIST_PARTITION1.CREATED_AT) "
        + "ORDER BY DT", hiveQuery);
  }*/
}
