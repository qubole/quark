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

package com.qubole.quark.catalog.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.QuarkFactory;
import com.qubole.quark.planner.QuarkFactoryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Sets up and organizes the planner with tables from all federated data sources.
 * The info argument encodes all the required information (described below) as
 * JSON.
 * Consider an example where there are 3 data sources:
 * <ul>
 * <li>CANONICAL</li>
 * <li>CUBES</li>
 * <li>VIEWS</li>
 * </ul>
 * CANONICAL is the default. Each of the data sources have many schemas and
 * tables. This class and its dependents set up such that the tables can be
 * referred as follows:
 * <ul>
 * <li><i>canonical.default.lineitem</i> refers to a table in DEFAULT in CANONICAL.</li>
 * <li><i>canonical.tpch.customers</i> refers to a table in TPCH in CANONICAL.</li>
 * <li><i>lineitem</i> refers to a table in DEFAULT in CANONICAL.</li>
 * <li><i>CUBES.PUBLIC.WEB_RETURNS</i> refers to a table in PUBLIC in CUBES.</li>
 * </ul>
 *
 */
public class SchemaFactory implements QuarkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaFactory.class);

  /**
   * Creates list of QuarkSchema
   *
   * @param info A JSON string which represents a RootSchema and its dependent objects.
   * @return
   * @throws QuarkException
   */
  public QuarkFactoryResult create(Properties info) throws QuarkException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      RootSchema rootSchema = objectMapper.readValue((String) info.get("model"), RootSchema.class);

      ImmutableList.Builder<DataSourceSchema> schemaList = new ImmutableList.Builder<>();

      for (com.qubole.quark.catalog.json.DataSourceSchema secondary : rootSchema.dataSources) {
        schemaList.add(secondary);
      }

      List<DataSourceSchema> schemas = schemaList.build();
      return new QuarkFactoryResult(schemas, rootSchema.relSchema,
          rootSchema.defaultDataSource != null ? schemas.get(rootSchema.defaultDataSource)
              : null
      );
    } catch (java.io.IOException e) {
      LOG.error("Unexpected Exception during create", e);
      throw new QuarkException(e);
    }
  }
}
