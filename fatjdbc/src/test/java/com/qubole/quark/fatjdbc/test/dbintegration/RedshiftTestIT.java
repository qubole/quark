package com.qubole.quark.fatjdbc.test.dbintegration;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by amoghm on 12/8/15.
 */
public class RedshiftTestIT {
  private final Connection conn = getConnection();

  private static Connection getConnection() {
    try {
      Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
      return DriverManager.getConnection("jdbc:quark:fat:json:"
          + System.getProperty("integrationTestResource") + "/redshiftModel.json",
          new Properties());
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException("Exception thrown on creating Quark connection: "
          + e.getMessage());
    }
  }

  @Test
  public void redshiftTest1() throws SQLException, ClassNotFoundException {
    String query="select * from redshift.public.nation_partition limit 1";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void redshiftTest2() throws SQLException, ClassNotFoundException {
    String query="select * from redshift.public.store_sales_partition limit 1";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void quboleRedshiftTest1() throws SQLException, ClassNotFoundException {
    String query="select * from qrs.public.nation_partition limit 1";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void quboleRedshiftTest2() throws SQLException, ClassNotFoundException {
    String query="select * from qrs.public.store_sales_partition limit 1";
    conn.createStatement().executeQuery(query);
  }
}
