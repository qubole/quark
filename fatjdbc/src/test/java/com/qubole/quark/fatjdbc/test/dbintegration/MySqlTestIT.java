package com.qubole.quark.fatjdbc.test.dbintegration;

import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by dev on 12/10/15.
 */
public class MySqlTestIT {

  private final Connection conn = getConnection();

  private static Connection getConnection() {
    try {
      Class.forName("com.qubole.quark.fatjdbc.QuarkDriver");
      return DriverManager.getConnection("jdbc:quark:fat:json:"
              + System.getProperty("integrationTestResource") + "/mySqlModel.json",
          new Properties());
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException("Exception thrown on creating quark connection: "
              + e.getMessage());
    }
  }

  @Test
  public void testBasicDataTypes() throws SQLException {
    /**
     * Datatypes checked:
     *
     * mysql> describe tab;
     +-------+---------------+------+-----+-------------------+-----------------------------+
     | Field | Type          | Null | Key | Default           | Extra                       |
     +-------+---------------+------+-----+-------------------+-----------------------------+
     | col1  | int(11)       | YES  |     | NULL              |                             |
     | col2  | tinyint(4)    | YES  |     | NULL              |                             |
     | col3  | smallint(6)   | YES  |     | NULL              |                             |
     | col4  | mediumint(9)  | YES  |     | NULL              |                             |
     | col5  | bigint(20)    | YES  |     | NULL              |                             |
     | col6  | float         | YES  |     | NULL              |                             |
     | col7  | double        | YES  |     | NULL              |                             |
     | col8  | decimal(10,0) | YES  |     | NULL              |                             |
     | col9  | date          | YES  |     | NULL              |                             |
     | col10 | datetime      | YES  |     | NULL              |                             |
     | col11 | timestamp     | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
     | col12 | time          | YES  |     | NULL              |                             |
     | col14 | char(1)       | YES  |     | NULL              |                             |
     | col15 | varchar(25)   | YES  |     | NULL              |                             |
     | col16 | text          | YES  |     | NULL              |                             |
     | col17 | tinytext      | YES  |     | NULL              |                             |
     | col18 | mediumtext    | YES  |     | NULL              |                             |
     | col19 | longtext      | YES  |     | NULL              |                             |
     +-------+---------------+------+-----+-------------------+-----------------------------+
     */
    String query = "select * from MYSQL.\"nezha_oss\".\"tab\"";
    conn.createStatement().executeQuery(query);
  }

}
