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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Describes the root element in the JSON template.
 */
public class RootSchema {
  public final List<String> supportedVersions = ImmutableList.of("2.0");
  public final List<DataSourceSchema> dataSources;
  public final Integer defaultDataSource;
  public final RelSchema relSchema;

  @JsonCreator
  public RootSchema(@JsonProperty("version") String version,
                    @JsonProperty("dataSources") List<DataSourceSchema> dataSources,
                    @JsonProperty("defaultDataSource") Integer defaultDataSource,
                    @JsonProperty("relSchema") RelSchema relSchema) {
    if (version == null) {
      throw new RuntimeException("version is required");
    }

    if (!supportedVersions.contains(version)) {
      throw new RuntimeException("Unsupported Version: '" + version + "'");
    }

    if (dataSources != null && !dataSources.isEmpty() && defaultDataSource == null) {
      throw new RuntimeException("Default DataSource must be specified");
    }
    this.dataSources = dataSources;
    this.defaultDataSource = defaultDataSource;
    this.relSchema = relSchema;
  }
}
