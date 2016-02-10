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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Describes the root element in the JSON template.
 */
public class RootSchema {
  private static final Logger LOG = LoggerFactory.getLogger(RootSchema.class);
  public final List<DataSourceSchema> dataSources;
  public final RelSchema relSchema;

  @JsonCreator
  public RootSchema(@JsonProperty("version") String version,
                    @JsonProperty("dataSources") List<DataSourceSchema> dataSources,
                    @JsonProperty("relSchema") RelSchema relSchema) {
    this.dataSources = dataSources;
    this.relSchema = relSchema;
  }
}
