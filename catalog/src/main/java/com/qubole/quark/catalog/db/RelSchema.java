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

package com.qubole.quark.catalog.db;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableList;

import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.QuarkCube;
import com.qubole.quark.planner.QuarkView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * An implementation of {@link MetadataSchema}
 * that is constructed from database
 */
public class RelSchema extends MetadataSchema {
  private static final Logger LOG = LoggerFactory.getLogger(RelSchema.class);

  public RelSchema() {
    super();
    this.views = ImmutableList.of();
    this.cubes = ImmutableList.of();
  }

  public RelSchema(List<DbView> views, List<DbCube> cubes) {
    super();
    this.views = new ArrayList<QuarkView>(views);
    this.cubes = new ArrayList<QuarkCube>(cubes);
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema,
                                  String name) {
    return Expressions.call(
        DataContext.ROOT,
        BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
  }

  /**
   * An implementation of {@link QuarkCube}
   * that is constructed from database
   */

  public static class DbCube extends QuarkCube {

    public DbCube(String name,
                  String query,
                  List<DbMeasure> dbMeasures,
                  List<DbDimension> dbDimensions,
                  List<DbGroup> dbGroups,
                  String destination,
                  String schemaName,
                  String tableName,
                  String groupingColumn
    ) {
      super(name, query, new ArrayList<Measure>(dbMeasures),
          ImmutableList.<Dimension>copyOf(dbDimensions),
          ImmutableList.<Group>copyOf(dbGroups),
          ImmutableList.of(destination, schemaName, tableName),
          groupingColumn,
          ImmutableList.of(schemaName, tableName));
    }
  }

  /**
   * An implementation of {@link QuarkCube.Dimension}
   * that is constructed from database
   */
  public static class DbDimension extends QuarkCube.Dimension {

    public DbDimension(String name,
                       String schemaName,
                       String tableName,
                       String columnName,
                       String cubeColumnName,
                       int dimensionOrder,
                       String parent) {
      super(name, schemaName, tableName, columnName,
          cubeColumnName, dimensionOrder, parent, null,
          new ArrayList<QuarkCube.Dimension>(), false);
    }
  }

  /**
   * An implementation of {@link QuarkCube.Group}
   * that is constructed from database
   */

  public static class DbGroup extends QuarkCube.Group {

    public DbGroup(String name, String dimensionName) {
      super(name, dimensionName);
    }
  }

  /**
   * An implementation of {@link QuarkCube.Measure}
   * that is constructed from database
   */

  public static class DbMeasure extends QuarkCube.Measure {

    public DbMeasure(String columnName,
                     String function,
                     String cubeColumnName) {
      super(function, columnName, cubeColumnName);
    }
  }

  /**
   * An implementation of {@link QuarkView}
   * that is constructed from database
   */

  public static class DbView extends QuarkView {

    public DbView(String name,
                  String viewSql,
                  String table,
                  String schema,
                  String destination) {
      super(name, viewSql, table, ImmutableList.of(destination, schema),
          ImmutableList.of(schema, table));
    }
  }
}
