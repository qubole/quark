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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.qubole.quark.QuarkException;

import com.qubole.quark.sql.QueryContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores name and {@link SchemaPlus} twin for all schemas
 */
public abstract class QuarkSchema extends AbstractSchema {
  private static final Logger LOG = LoggerFactory.getLogger(QuarkSchema.class);

  final String name;
  protected SchemaPlus schemaPlus;
  public QuarkSchema(String name) {
    super();
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public abstract void initialize(QueryContext queryContext, SchemaPlus schemaPlus)
      throws QuarkException;
}
