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

import com.qubole.quark.QuarkException;
import com.qubole.quark.plugin.DataSource;
import com.qubole.quark.plugin.DataSourceFactory;

import java.util.Map;

/**
 * Created by rajatv on 11/12/15.
 */
public class JdbcFactory implements DataSourceFactory {
  public DataSource create(Map<String, Object> properties) throws QuarkException {
    String type = (String) properties.get("type");
    String url = (String) properties.get("url");
    String user = (String) properties.get("username");
    String password = (String) properties.get("password");

    if (type.toUpperCase().equals("HIVE")) {
      return new EMRDb((String) properties.get("jdbcUrl"), url, user, password,
              (String) properties.get("driverName"));
    } else if (type.toUpperCase().equals("REDSHIFT")) {
      return new RedShiftDb(url, user, password);
    } else if (type.toUpperCase().equals("H2")) {
      return new H2Db(url, user, password);
    } else if (type.toUpperCase().equals("MYSQL")) {
      return new MysqlDb(url, user, password);
    } else if (type.toUpperCase().equals("ORACLE")) {
      return new OracleDb(url, user, password);
    }
    throw new QuarkException(new Throwable("Invalid DataSource type:" + type));
  }
}
