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

package com.qubole.quark.catalog.db.pojo;

/**
 * An intermediary class to load {@link com.qubole.quark.catalog.db.RelSchema.DbCube}
 * from database.
 */
public class Cube {

  private long id;
  private String name;
  private String query;
  private String schemaName;
  private String tableName;
  private String groupingColumn;
  private String destination;

  public Cube(long id,
              String name,
              String query,
              String schemaName,
              String tableName,
              String groupingColumn,
              String destination) {
    this.id = id;
    this.name = name;
    this.query = query;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.groupingColumn = groupingColumn;
    this.destination = destination;
  }

  public String getDestination() {
    return destination;
  }

  public String getGroupingColumn() {
    return groupingColumn;
  }

  public String getTableName() {
    return tableName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getQuery() {
    return query;
  }

  public String getName() {
    return name;
  }

  public long getId() {
    return this.id;
  }
}
