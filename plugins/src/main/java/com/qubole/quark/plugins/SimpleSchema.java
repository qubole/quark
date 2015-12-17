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

package com.qubole.quark.plugins;

import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableMap;

import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.sql.QueryContext;

import java.util.Map;

/**
 * Holds only a list of {@link QuarkTable}
 */
public class SimpleSchema extends QuarkSchema {
  private final ImmutableMap<String, Table> tableMap;

  public SimpleSchema(String name, ImmutableMap<String, Table> tableMap) {
    super(name);
    this.tableMap = tableMap;
  }

  @Override
  public void initialize(QueryContext queryContext) {}

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }
}
