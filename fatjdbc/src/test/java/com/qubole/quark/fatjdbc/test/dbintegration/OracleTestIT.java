package com.qubole.quark.fatjdbc.test.dbintegration;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by amoghm on 12/8/15.
 */
@Ignore("NEZ-74")
public class OracleTestIT {
  private final Connection conn = getConnection();

  private static Connection getConnection() {
    try {
      Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
      return DriverManager.getConnection("jdbc:quark:fat:json:"
          + System.getProperty("integrationTestResource") + "/oracleModel.json",
          new Properties());
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException("Exception thrown on creating Quark connection: "
          + e.getMessage());
    }
  }

  @Test
  public void oracleTypeTest1() throws SQLException, ClassNotFoundException {
    String query="select * from tab";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest2() throws SQLException, ClassNotFoundException {
    String query="select * from tab2";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest3() throws SQLException, ClassNotFoundException {
    String query="select * from tab3";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest4() throws SQLException, ClassNotFoundException {
    String query="select * from tab4";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest5() throws SQLException, ClassNotFoundException {
    String query="select * from tab5";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest6() throws SQLException, ClassNotFoundException {
    String query="select * from tab6";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest7() throws SQLException, ClassNotFoundException {
    String query="select * from ansi";
    conn.createStatement().executeQuery(query);
  }

  @Test
  public void oracleTypeTest8() throws SQLException, ClassNotFoundException {
    String query="select * from ansi2";
    conn.createStatement().executeQuery(query);
  }
}
