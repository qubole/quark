package com.qubole.quark.fatjdbc.test.dbintegration;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by amoghm on 12/8/15.
 */
public class SparkTestIT {
  private final Connection conn = getConnection();

  private static Connection getConnection() {
    try {
      Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
      return DriverManager.getConnection("jdbc:quark:fat:json:"
          + System.getProperty("integrationTestResource") + "/SparkModel.json",
          new Properties());
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException("Exception thrown on creating quark connection: "
          + e.getMessage());
    }
  }
  @Test
  public void testSparkSql() throws Exception {
    String sql = "select\n" +
        "to_date(qh.created_at) as dt, \n" +
        "count(qh.id) as num_queries\n" +
        "\n" +
        "from hive.default.query_hists qh\n" +
        "join hive.default.user_info ui\n" +
        "  on qh.qbol_user_id = ui.qu_id\n" +
        "join hive.default.canonical_accounts externals\n" +
        "  on externals.id = ui.a_id\n" +
        "\n" +
        "where to_date(qh.created_at) >= date_sub(from_unixtime(unix_timestamp()),30)\n" +
        "and   command_type = 'HiveCommand'\n" +
        "and   qlog like '%\\\"HIVE_VERSION\\\":\\\"1.2\\\"%'\n" +
        "and   customer_name like '%'\n" +
        "\n" +
        "group by \n" +
        "to_date(qh.created_at)\n" +
        "\n" +
        "order by dt asc";
    conn.createStatement().executeQuery(sql);
  }
}
