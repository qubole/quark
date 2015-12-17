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

package com.qubole.quark.plugin;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableMap;

import com.qubole.quark.QuarkException;


/**
 * A representation of a instance of a database or a data store. No assumptions are made on the
 * underlying technology. The only constraint is that the structure of the data can be
 * represented by schemas, tables and columns with a well-known data type.
 */
public interface DataSource {
  ImmutableMap<String, Schema> getSchemas() throws QuarkException;
  String getDefaultSchema();
  String getProductName();
  SqlDialect getSqlDialect();
  boolean isCaseSensitive();
}
