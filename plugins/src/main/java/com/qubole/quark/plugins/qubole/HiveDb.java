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

import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableMap;
import com.qubole.qds.sdk.java.client.QdsClient;
import com.qubole.qds.sdk.java.client.ResultLatch;
import com.qubole.qds.sdk.java.entities.CommandResponse;
import com.qubole.qds.sdk.java.entities.ResultValue;
import com.qubole.qds.sdk.java.entities.SchemaListDescribed;
import com.qubole.qds.sdk.java.entities.SchemaOrdinal;

import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by dev on 11/13/15.
 */
public class HiveDb extends QuboleDB {

  private final String defaultSchema = "DEFAULT";
  private final String productName = "HIVE";
  private final boolean isCaseSensitive = false;

  private static final ImmutableMap<String, Integer> HIVE_DATA_TYPES =
          new ImmutableMap.Builder<String, Integer>()
                  .put("character varying\\([0-9]+\\)", Types.VARCHAR)
                  .put("varchar\\([0-9]+\\)", Types.VARCHAR)
                  .put("char\\([0-9]+\\)", Types.CHAR)
                  .put("character\\([0-9]+\\)", Types.CHAR)
                  .put("decimal\\([0-9]+,[0-9]+\\)", Types.DECIMAL)
                  .put("timestamp", Types.TIMESTAMP).build();

  public HiveDb(Map<String, Object> properties) {
    super(properties);
  }

  @Override
  protected Map<String, List<SchemaOrdinal>> getSchemaListDescribed()
          throws ExecutionException, InterruptedException {

    SchemaListDescribed schemaListDescribed = getQdsClient().hiveMetadata()
            .getSchemaListDescribed().invoke().get();
    Map<String, List<SchemaOrdinal>> schemas = schemaListDescribed.getSchemas();

    int per_page = schemaListDescribed.getPaging_info().getPer_page();
    int current_page = 2;

    while (schemaListDescribed.getPaging_info().getNext_page() != null) {
      schemaListDescribed =  getQdsClient().hiveMetadata().getSchemaListDescribed().
              forPage(current_page, per_page).invoke().get();
      schemas.putAll(schemaListDescribed.getSchemas());
    }
    return schemas;
  }

  @Override
  protected ImmutableMap<String, Integer> getDataTypes() {
    return HIVE_DATA_TYPES;
  }

  @Override
  public String getDefaultSchema() {
    return defaultSchema;
  }

  @Override
  public String getProductName() {
    return productName;
  }

  @Override
  public SqlDialect getSqlDialect() {
    final SqlDialect hiveDialect =
            SqlDialect.getProduct("Hive", null).getDialect();
    return hiveDialect;
  }

  @Override
  public Iterator<Object> executeQuery(String sql) throws Exception {

    QdsClient client  = getQdsClient();
    ResultValue resultValue;
    try {
      CommandResponse commandResponse = client.command().hive().query(sql).invoke().get();
      ResultLatch resultLatch = new ResultLatch(client, commandResponse.getId());
      resultValue = resultLatch.awaitResult();
    } finally {
      client.close();
    }
    return convertToIterator(resultValue);
  }

  public boolean isCaseSensitive() {
    return isCaseSensitive;
  }
}
