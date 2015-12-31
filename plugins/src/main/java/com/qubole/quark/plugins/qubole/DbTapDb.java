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

import org.apache.commons.lang.Validate;

import com.google.common.collect.ImmutableMap;

import com.qubole.qds.sdk.java.client.QdsClient;
import com.qubole.qds.sdk.java.client.ResultLatch;
import com.qubole.qds.sdk.java.entities.CommandResponse;
import com.qubole.qds.sdk.java.entities.DbTap;
import com.qubole.qds.sdk.java.entities.ResultValue;
import com.qubole.qds.sdk.java.entities.SchemaList;
import com.qubole.qds.sdk.java.entities.SchemaListDescribed;
import com.qubole.qds.sdk.java.entities.SchemaOrdinal;

import com.qubole.quark.plugins.jdbc.MysqlDb;
import com.qubole.quark.plugins.jdbc.RedShiftDb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by dev on 11/13/15.
 */
public class DbTapDb extends QuboleDB {

  protected int dbTapid;
  private String productName = null;
  private String defaultSchema = null;

  public DbTapDb(Map<String, Object> properties) {
    super(properties);
    Validate.notNull(properties.get("dbtapid"),
        "Field \"dbtapid\" specifying Qubole's endpoint needs "
        + "to be defined for Qubole Data Source of type DBTAP in JSON");
    this.dbTapid = Integer.parseInt(properties.get("dbtapid").toString());
  }

  @Override
  protected Map<String, List<SchemaOrdinal>> getSchemaListDescribed() throws
          ExecutionException, InterruptedException {

    SchemaListDescribed schemaListDescribed = getQdsClient().dbTaps().
            getSchemaListDescribed(dbTapid).invoke().get();
    Map<String, List<SchemaOrdinal>> schemas = schemaListDescribed.getSchemas();

    int per_page = schemaListDescribed.getPaging_info().getPer_page();
    int current_page = 2;

    while (schemaListDescribed.getPaging_info().getNext_page() != null) {
      schemaListDescribed =  getQdsClient().dbTaps().getSchemaListDescribed(dbTapid)
              .forPage(current_page++, per_page).invoke().get();
      schemas.putAll(schemaListDescribed.getSchemas());
    }
    return schemas;
  }

  @Override
  protected ImmutableMap<String, String> getDataTypes() {
    String type = this.getProductName();
    switch (type.toUpperCase()) {
      case "REDSHIFT":
        return  RedShiftDb.DATATYPES;
      case "SQLSERVER":
      case "MYSQL":
        return MysqlDb.DATATYPES;
      default:
        return ImmutableMap.of();
    }
  }

  @Override
  public String getDefaultSchema() {
    /**
     * TODO: We should have fetched default schema in
     * function getSchemaListDescribed
     * but that API currently doesnt return defaultSchema
     * and needs to be extended
     */
    try {
      SchemaList schemaList =
              getQdsClient().dbTaps().getSchemaNames(dbTapid).invoke().get();
      this.defaultSchema = schemaList.getDefault_schema().toUpperCase();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Couldnot fetch defaultSchema for Qubole DataSource id: "
                + dbTapid + " Error:" + e.getMessage(), e);
    }
    return this.defaultSchema;
  }

  @Override
  public String getProductName() {
    if (this.productName == null) {
      try {
        DbTap dbTap = getQdsClient().dbTaps().view(dbTapid).invoke().get();
        this.productName = dbTap.getDb_type().toUpperCase();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Couldnot fetch productName "
            + "for Qubole DataSource id: "
            + dbTapid + " Error:" + e.getMessage(), e);
      }
    }
    return this.productName;
  }

  @Override
  public SqlDialect getSqlDialect() {
    String type = this.getProductName();
    final SqlDialect dialect;
    switch (type.toUpperCase()) {
      case "HIVE":
        dialect = SqlDialect.getProduct("Hive", null).getDialect();
        break;
      case "REDSHIFT":
        dialect = SqlDialect.getProduct("REDSHIFT", null).getDialect();
        break;
      case "SQLSERVER":
      case "MYSQL":
        dialect = SqlDialect.getProduct("MySQL", null).getDialect();
        break;
      case "POSTGRESQL":
        dialect = SqlDialect.getProduct("PostgreSQL", null).getDialect();
        break;
      case "VERTICA":
        dialect = SqlDialect.getProduct("Vertica", null).getDialect();
        break;
      default:
        dialect = SqlDialect.getProduct("UNKNOWN", null).getDialect();
        break;
    }
    dialect.setUseLimitKeyWord(true);
    return dialect;
  }

  public Iterator<Object> executeQuery(String sql) throws Exception {

    QdsClient client = getQdsClient();
    ResultValue resultValue;

    try {
      CommandResponse commandResponse = client.command().dbTapQuery(sql, dbTapid).invoke().get();
      ResultLatch resultLatch = new ResultLatch(client, commandResponse.getId());
      resultValue = resultLatch.awaitResult();
    } finally {
      client.close();
    }
    return convertToIterator(resultValue);
  }

  public boolean isCaseSensitive() {
    if (this.getProductName().toUpperCase().equals("MYSQL")) {
      return true;
    }
    return false;
  }
}
