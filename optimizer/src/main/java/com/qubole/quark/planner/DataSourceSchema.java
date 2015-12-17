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

package com.qubole.quark.planner;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableMap;

import com.qubole.quark.QuarkException;

import com.qubole.quark.plugin.DataSource;
import com.qubole.quark.plugin.DataSourceFactory;
import com.qubole.quark.sql.QueryContext;

import java.util.Map;

/**
 * Stores sub-schemas read from a {@link DataSource}.
 * It never holds any tables.
 */
public abstract class DataSourceSchema extends QuarkSchema {
  public final boolean isDefault;
  public final Map<String, Object> properties;
  protected ImmutableMap<String, Schema> subSchemaMap;
  private final DataSourceFactory dataSourceFactory;
  private DataSource dataSource = null;

  public DataSourceSchema(Map<String, Object> properties) {
    super((String) properties.get("name"));
    this.properties = properties;
    this.isDefault = Boolean.parseBoolean((String) properties.get("default"));
    final String factoryPath = (String) properties.get("factory");
    if (factoryPath == null) {
      throw new RuntimeException("Specify attribute in user "
          + "JSON input - 'factory': <ClassPath of DataSourceFactory Implemented>");
    }
    try {
      Class dsFactoryClazz = Class.forName(factoryPath);
      this.dataSourceFactory =
          (DataSourceFactory) dsFactoryClazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Error parsing JSON attribute 'factory': " + e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    return subSchemaMap;
  }

  public DataSource getDataSource() throws QuarkException {
    if (dataSource == null) {
      dataSource = dataSourceFactory.create(properties);
    }
    return dataSource;
  }

  @Override
  public void initialize(QueryContext queryContext) throws QuarkException {
    subSchemaMap = this.getDataSource().getSchemas();
  }

  public boolean isDefault() {
    return isDefault;
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema,
                                  String name) {
    return Expressions.call(
        DataContext.ROOT,
        BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
  }
}
