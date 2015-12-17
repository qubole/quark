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

package com.qubole.quark.jdbc.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.BuiltInMethod;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
 * that is constructed from JSON
 */
public class RelSchema extends MetadataSchema {
  private static final Logger LOG = LoggerFactory.getLogger(RelSchema.class);

  public RelSchema() {
    super();
    this.views = ImmutableList.of();
    this.cubes = ImmutableList.of();
  }

  @JsonCreator
  public RelSchema(@JsonProperty("views") List<JsonView> views,
                   @JsonProperty("cubes") List<JsonCube> cubes) {
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
   * that is constructed from JSON
   */

  public static class JsonCube extends QuarkCube {
    @JsonCreator
    public JsonCube(@JsonProperty("name") String name,
                    @JsonProperty("query") String query,
                    @JsonProperty("measures") List<JsonMeasure> jsonMeasures,
                    @JsonProperty("dimensions") List<JsonDimension> jsonDimensions,
                    @JsonProperty("groups") List<JsonGroup> jsonGroups,
                    @JsonProperty("dataSource") String destination,
                    @JsonProperty("schema") String schemaName,
                    @JsonProperty("table") String tableName,
                    @JsonProperty("groupingColumn") String groupingColumn
                    ) {
      super(name, query, new ArrayList<Measure>(jsonMeasures),
          ImmutableList.<Dimension>copyOf(jsonDimensions),
          ImmutableList.<Group>copyOf(jsonGroups),
          ImmutableList.of(destination, schemaName, tableName),
          groupingColumn,
          ImmutableList.of(schemaName, tableName));
    }
  }

  /**
   * An implementation of {@link QuarkCube.Dimension}
   * that is constructed from JSON
   */
  public static class JsonDimension extends QuarkCube.Dimension {
    @JsonCreator
    public JsonDimension(@JsonProperty("name") String name,
                         @JsonProperty("schema") String schemaName,
                         @JsonProperty("table") String tableName,
                         @JsonProperty("column") String columnName,
                         @JsonProperty("cubeColumn") String cubeColumnName,
                         @JsonProperty("dimensionOrder") int dimensionOrder,
                         @JsonProperty("parent") String parent) {
      super(name, schemaName, tableName, columnName, cubeColumnName, dimensionOrder, parent);
    }
  }

  /**
   * An implementation of {@link QuarkCube.Group}
   * that is constructed from JSON
   */

  public static class JsonGroup extends QuarkCube.Group {

    @JsonCreator
    public JsonGroup(@JsonProperty("name") String name,
                     @JsonProperty("dimensionName") String dimensionName) {
      super(name, dimensionName);
    }
  }

  /**
   * An implementation of {@link QuarkCube.Measure}
   * that is constructed from JSON
   */

  public static class JsonMeasure extends QuarkCube.Measure {
    @JsonCreator
    public JsonMeasure(@JsonProperty("column") String columnName,
                       @JsonProperty("function") String function,
                       @JsonProperty("cubeColumn") String cubeColumnName) {
      super(function, columnName, cubeColumnName);
    }
  }

  /**
   * An implementation of {@link QuarkView}
   * that is constructed from JSON
   */

  public static class JsonView extends QuarkView {
    @JsonCreator
    public JsonView(@JsonProperty("name") String name,
                    @JsonProperty("query") String viewSql,
                    @JsonProperty("table") String table,
                    @JsonProperty("schema") String schema,
                    @JsonProperty("dataSource") String destination) {
      super(name, viewSql, table, ImmutableList.of(destination, schema),
          ImmutableList.of(schema, table));
    }
  }
}
