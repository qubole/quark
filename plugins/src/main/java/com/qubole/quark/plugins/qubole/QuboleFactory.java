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

package com.qubole.quark.plugins.qubole;

import org.apache.commons.lang.Validate;

import com.google.common.collect.ImmutableMap;

import com.qubole.quark.QuarkException;
import com.qubole.quark.plugin.DataSource;
import com.qubole.quark.plugin.DataSourceFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by dev on 11/13/15.
 */
public class QuboleFactory implements DataSourceFactory {
  // Static Registry for Qubole DB plugin
  private static final Map<String, Class<? extends QuboleDB>> DB_PLUGINS =
      ImmutableMap.of(
          "HIVE", HiveDb.class,
          "DBTAP", DbTapDb.class
      );

  @Override
  public DataSource create(Map<String, Object> properties) throws QuarkException {

    Validate.notNull(properties.get("type"),
        "Field \"type\" specifying either HIVE or DBTAP needs "
            + "to be defined for Qubole Data Source in JSON");
    String type = (String) properties.get("type");
    Class dbClass = DB_PLUGINS.get(type.toUpperCase());
    Validate.notNull(dbClass, "Invalid DataSource type: " + type
        + " Please specify one of these: "
        + Arrays.toString(DB_PLUGINS.keySet().toArray()));
    return getDataSource(properties, dbClass);
  }

  private DataSource getDataSource(Map<String, Object> properties,
                                   Class dbClass) throws QuarkException {
    try {
      return (DataSource) (dbClass.getConstructor(Map.class)
          .newInstance(properties));
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException e) {
      throw new QuarkException(new Throwable("Invoking invalid constructor on class "
          + "specified for type " + properties.get("type") + ": " + dbClass.getCanonicalName()));
    } catch (InvocationTargetException e) {
      throw new QuarkException(e.getTargetException());
    }
  }
}
