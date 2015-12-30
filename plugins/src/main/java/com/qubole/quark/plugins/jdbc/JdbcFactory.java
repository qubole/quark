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

import org.apache.commons.lang.Validate;

import com.google.common.collect.ImmutableMap;

import com.qubole.quark.QuarkException;
import com.qubole.quark.plugin.DataSource;
import com.qubole.quark.plugin.DataSourceFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by rajatv on 11/12/15.
 */
public class JdbcFactory implements DataSourceFactory {
  // Static Registry for JDBC DB plugin
  private static final Map<String, Class<? extends JdbcDB>> DB_PLUGINS =
      ImmutableMap.of(
      "HIVE", EMRDb.class,
      "REDSHIFT", RedShiftDb.class,
      "H2", H2Db.class,
      "MYSQL", MysqlDb.class,
      "ORACLE", OracleDb.class);

  public DataSource create(Map<String, Object> properties) throws QuarkException {
    validate(properties);
    String type = (String) properties.get("type");
    String url = (String) properties.get("url");
    String user = (String) properties.get("username");
    String password = (String) properties.get("password");

    Class dbClass = DB_PLUGINS.get(type.toUpperCase());
    Validate.notNull(dbClass, "Invalid DataSource type: " + type
        + " Please specify one of these: "
        + Arrays.toString(DB_PLUGINS.keySet().toArray()));
    return getDataSource(type, url, user, password, dbClass);
  }

  private DataSource getDataSource(String type,
                                   String url,
                                   String user,
                                   String password,
                                   Class dbClass) throws QuarkException {
    try {
      Class[] parameters = new Class[] {String.class, String.class, String.class};
      return (DataSource) (dbClass.getConstructor(parameters)
          .newInstance(url, user, password));
    } catch (NoSuchMethodException | IllegalAccessException
        | InstantiationException | InvocationTargetException e) {
      throw new QuarkException(new Throwable("Invoking invalid constructor on class "
          + "specified for type " + type.toUpperCase() + ": " + dbClass.getCanonicalName()));
    }
  }

  private void validate(Map<String, Object> properties) {
    Validate.notNull(properties.get("type"),
        "Field \"type\" specifying type of DataSource endpoint needs "
        + "to be defined for JDBC Data Source in JSON");
    Validate.notNull(properties.get("url"), "Field \"url\" specifying JDBC endpoint needs "
        + "to be defined for JDBC Data Source in JSON");
    Validate.notNull(properties.get("username"), "Field \"username\" specifying username needs "
        + "to be defined for JDBC Data Source in JSON");
    Validate.notNull(properties.get("password"), "Field \"password\" specifying password "
        + "to be defined for JDBC Data Source in JSON");
  }
}
