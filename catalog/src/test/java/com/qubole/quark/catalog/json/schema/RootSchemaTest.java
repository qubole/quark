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

package com.qubole.quark.catalog.json.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qubole.quark.catalog.json.RootSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rajatv on 11/9/15.
 */
public class RootSchemaTest {
  private static final Logger log = LoggerFactory.getLogger(RootSchemaTest.class);


  @Test
  public void testOneDataSource() throws IOException {
    String jsonTestString =
        "{" +
            "\"version\":\"2.0\"," +
            "\"dataSources\":" +
            " [" +
            "   {" +
            "     \"type\":\"HIVE\"," +
            "     \"url\":\"http://localhost:3306\"," +
            "     \"factory\":\"com.qubole.quark.plugins.jdbc.JdbcFactory\"," +
            "     \"username\":\"root\"," +
            "     \"password\":\"ABCDEF\"," +
            "     \"name\":\"qubole_default\"" +
            "   }" +
            " ]," +
            " \"defaultDataSource\":1" +
            "}";


    ObjectMapper objectMapper = new ObjectMapper();
    RootSchema rootSchema = objectMapper.readValue(jsonTestString, RootSchema.class);

    assertThat(rootSchema.dataSources.size()).isEqualTo(1);
  }

  @Test
  public void testOnePartition() throws IOException {
    String jsonTestString =
        "{" +
            "  \"version\":\"2.0\"," +
            "  \"relSchema\":{" +
            "    \"views\" : [" +
            "      {" +
            "        \"name\":\"sampleView\"," +
            "        \"query\":\"select * from table\"," +
            "        \"table\":\"viewTable\"," +
            "        \"schema\": \"PUBLIC\"," +
            "        \"dataSource\": \"VIEWS\"" +
            "      }" +
            "    ]," +
            "    \"cubes\":[]" +
            "  }" +
            "}";
    ObjectMapper objectMapper = new ObjectMapper();
    RootSchema rootSchema = objectMapper.readValue(jsonTestString, RootSchema.class);

    assertThat(rootSchema.relSchema.getViews().size()).isEqualTo(1);
  }
}