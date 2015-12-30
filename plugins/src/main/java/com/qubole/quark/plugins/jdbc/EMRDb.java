/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.plugins.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by dev on 11/16/15.
 */
public class EMRDb extends HiveDb {

  protected  String jdbcUrl;
  private Connection connection;
  public String driverName;

  public EMRDb(String jdbcUrl, String url, String user, String password, String driverName) {
    super(url, user, password);
    this.jdbcUrl = jdbcUrl;
    this.driverName = driverName;
  }

  public Connection getConnectionExec() throws ClassNotFoundException, SQLException {
    Class.forName(driverName);
    return DriverManager.getConnection(jdbcUrl, user, password);
  }

  @Override
  public Iterator<Object> executeQuery(final String sql)
      throws Exception {
    cleanup();
    this.connection = this.getConnectionExec();
    return execute(this.connection, sql);
  }
}
