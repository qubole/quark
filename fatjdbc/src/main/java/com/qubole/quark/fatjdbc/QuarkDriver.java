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

package com.qubole.quark.fatjdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteRootSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qubole.quark.fatjdbc.impl.QuarkHandler;
import com.qubole.quark.fatjdbc.utility.CatalogDetail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Main class of Quark JDBC driver.
 */
public class QuarkDriver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:quark:fat:";

  static {
    new QuarkDriver().register();
  }

  public QuarkDriver() {
    super();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
      case JDBC_30:
      case JDBC_40:
        throw new IllegalArgumentException("JDBC version not supported: "
            + jdbcVersion);
      case JDBC_41:
      default:
        return "com.qubole.quark.fatjdbc.QuarkJdbc41Factory";
    }
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        QuarkDriver.class,
        "org-apache-calcite-jdbc.properties",
        "Quark JDBC Driver",
        "unknown version",
        "Quark",
        "unknown version");
  }

  @Override
  protected Handler createHandler() {
    return new QuarkHandler();
  }

  @Override
  protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, BuiltInConnectionProperty.values());
    Collections.addAll(list, CalciteConnectionProperty.values());
    return list;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new QuarkMetaImpl((QuarkConnectionImpl) connection,
        ((QuarkConnectionImpl) connection).getProperties());
  }

  public String getJsonSchemaFactoryPath() {
    return "com.qubole.quark.catalog.json.SchemaFactory";
  }

  public String getDbSchemaFactoryPath() {
    return "com.qubole.quark.catalog.db.SchemaFactory";
  }

  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    if (info.getProperty("model") == null && info.getProperty("dbCredentials") == null) {
      final String prefix = getConnectStringPrefix();
      final String urlSuffix = url.substring(prefix.length());
      try {
        byte[] encoded = Files.readAllBytes(Paths.get(urlSuffix));
        ObjectMapper objectMapper = new ObjectMapper();
        CatalogDetail catalogDetail = objectMapper.readValue(encoded, CatalogDetail.class);

        if (catalogDetail.dbCredentials != null) {
          info.put("dbCredentials",
              objectMapper.writeValueAsString(catalogDetail.dbCredentials));
        } else {
          info.setProperty("model", new String(encoded, StandardCharsets.UTF_8));
        }
      } catch (IOException e) {
        throw new SQLException(e.getMessage());
      }
    }

    if (info.getProperty("dbCredentials") == null) {
      info.setProperty("schemaFactory", getJsonSchemaFactoryPath());
    } else {
      info.setProperty("schemaFactory", getDbSchemaFactoryPath());
    }

    return super.connect(url, info);
  }

  /**
   * Creates an internal connection.
   */
  QuarkConnection connect(CalciteRootSchema rootSchema,
                          JavaTypeFactory typeFactory) throws SQLException {
    return (QuarkConnection) ((QuarkJdbcFactory) factory)
        .newConnection(this, (QuarkJdbcFactory) factory, CONNECT_STRING_PREFIX, new Properties(),
            rootSchema, typeFactory);
  }
}
