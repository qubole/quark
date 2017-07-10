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

package com.qubole.quark.plugins.qubole;

import org.apache.calcite.schema.Schema;

import org.apache.commons.lang.Validate;

import com.google.common.collect.ImmutableMap;

import com.qubole.qds.sdk.java.client.DefaultQdsConfiguration;
import com.qubole.qds.sdk.java.client.QdsClient;
import com.qubole.qds.sdk.java.client.QdsClientFactory;
import com.qubole.qds.sdk.java.client.QdsConfiguration;
import com.qubole.qds.sdk.java.entities.ResultValue;
import com.qubole.qds.sdk.java.entities.SchemaOrdinal;

import com.qubole.quark.QuarkException;
import com.qubole.quark.plugins.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by dev on 11/13/15.
 */
public abstract class QuboleDB implements Executor {
  private static final Logger LOG = LoggerFactory.getLogger(QuboleDB.class.getName());
  private static final String ROW_DELIMETER = "\r\n";
  private static final String COLUMN_DELIMETER = "\t";
  protected String token;
  protected String endpoint;
  QuboleDB(Map<String, Object> properties) {
    validate(properties);
    this.endpoint = (String) properties.get("endpoint");
    this.token = (String) properties.get("token");
  }

  protected abstract Map<String, List<SchemaOrdinal>>
                        getSchemaListDescribed() throws ExecutionException, InterruptedException;

  protected abstract ImmutableMap<String, Integer> getDataTypes();

  public abstract boolean isCaseSensitive();

  private void validate(Map<String, Object> properties) {
    Validate.notNull(properties.get("endpoint"),
        "Field \"endpoint\" specifying Qubole's endpoint needs "
            + "to be defined for Qubole Data Source in JSON");
    Validate.notNull(properties.get("token"),
        "Field \"token\" specifying Authentication token needs "
            + "to be defined for Qubole Data Source in JSON");
  }



  protected QdsClient getQdsClient() {
    QdsConfiguration configuration = new DefaultQdsConfiguration(endpoint, token);
    return QdsClientFactory.newClient(configuration);
  }

  @Override
  public void cleanup() throws Exception {}

  @Override
  public ImmutableMap<String, Schema> getSchemas() throws QuarkException {
    Map<String, List<SchemaOrdinal>> schemas;
    try {
      schemas = getSchemaListDescribed();
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Getting Schema metadata for " + this.endpoint
          + " failed. Error: " + e.getMessage(), e);
      throw new QuarkException(e);
    }
    ImmutableMap.Builder<String, Schema> schemaBuilder = new ImmutableMap.Builder<>();

    for (String schemaName: schemas.keySet()) {
      String schemaKey = schemaName;
      if (!this.isCaseSensitive()) {
        schemaKey = schemaName.toUpperCase();
      }
      schemaBuilder.put(schemaKey, new QuboleSchema(schemaKey,
          schemas.get(schemaName), this.isCaseSensitive(), this.getDataTypes()));
    }
    return schemaBuilder.build();
  }

  protected Iterator<Object> convertToIterator(ResultValue resultValue) {
    String[] result = resultValue.getResults().split(ROW_DELIMETER);
    List<Object> resultSet = new ArrayList<>();

    for (int i = 0; i < result.length; i++) {
      //check if Empty Result Set
      // QDS returns row of empty column
      if (result[i].length() == 0) {
        resultSet = new ArrayList<>();
        break;
      }
      String[] row = result[i].split(COLUMN_DELIMETER);
      resultSet.add(row);
    }
    return resultSet.iterator();
  }

  /**
   * Merges two schema Maps. 'putAll' method can't be used because if the any of the
   * key is same in the maps, then it will get overwritten.
   * for e.g.
   * if map1 = {'public' -&gt; list1}, map2 = {'public' -&gt; list2}
   * then
   *   map1.putAll(map2) will return map2 ({'public' -&gt; list2})
   *   i.e. map1 gets overwritten
   *
   * @param schemas1 schema map1
   * @param schemas2 schema map2
   *
   * @return returns merged Map
   * */
  protected Map<String, List<SchemaOrdinal>> mergeSchemas(
      Map<String, List<SchemaOrdinal>> schemas1,
      Map<String, List<SchemaOrdinal>> schemas2) {
    for (String schemaName : schemas2.keySet()) {
      if (schemas1.containsKey(schemaName)) {
        schemas1.get(schemaName).addAll(schemas2.get(schemaName));
      } else {
        schemas1.put(schemaName, schemas2.get(schemaName));
      }
    }
    return schemas1;
  }

}
