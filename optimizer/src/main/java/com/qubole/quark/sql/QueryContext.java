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

package com.qubole.quark.sql;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.ObjectMapper;


import com.google.common.collect.ImmutableList;

import com.qubole.quark.QuarkException;

import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.QuarkFactory;
import com.qubole.quark.planner.QuarkFactoryResult;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.TestFactory;
import com.qubole.quark.plugin.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by amargoor on 10/26/15.
 */
public class QueryContext {

  protected static final Logger LOG = LoggerFactory.getLogger(QueryContext.class);
  private final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true).plus();
  private DataSourceSchema defaultDataSource;
  private boolean unitTestMode;
  private final List<String> defaultSchema;

  public CalciteConnectionConfig getCfg() {
    return cfg;
  }

  private final CalciteConnectionConfig cfg;

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  private final JavaTypeFactory typeFactory;

  private List<String> parseDefaultSchema(Properties info) throws IOException, QuarkException {
    if (info.getProperty("defaultSchema") != null) {
      final ObjectMapper mapper = new ObjectMapper();
      return Arrays.asList(mapper.readValue(info.getProperty("defaultSchema"), String[].class));
    } else if (defaultDataSource != null) {
      DataSource dataSource = defaultDataSource.getDataSource();
      return ImmutableList.of(defaultDataSource.getName(), dataSource.getDefaultSchema());
    } else {
      return ImmutableList.of(MetadataSchema.NAME);
    }
  }

  public QueryContext(Properties info) throws QuarkException {
    try {
      Class schemaFactoryClazz = Class.forName(info.getProperty("schemaFactory"));
      this.cfg = new CalciteConnectionConfigImpl(info);
      final RelDataTypeSystem typeSystem =
          cfg.typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);
      this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
      this.unitTestMode = Boolean.parseBoolean(info.getProperty("unitTestMode", "false"));

      Object obj = schemaFactoryClazz.newInstance();
      if (obj instanceof QuarkFactory) {
        final QuarkFactory schemaFactory = (QuarkFactory) schemaFactoryClazz.newInstance();
        QuarkFactoryResult factoryResult = schemaFactory.create(info);
        for (DataSourceSchema schema : factoryResult.dataSourceSchemas) {
          SchemaPlus schemaPlus = rootSchema.add(schema.getName(), schema);
          schema.setSchemaPlus(schemaPlus);
        }

        SchemaPlus metadataPlus = rootSchema.add(factoryResult.metadataSchema.getName(),
            factoryResult.metadataSchema);
        factoryResult.metadataSchema.setSchemaPlus(metadataPlus);

        for (DataSourceSchema dataSourceSchema : factoryResult.dataSourceSchemas) {
          dataSourceSchema.initialize(this);
        }
        factoryResult.metadataSchema.initialize(this);

        this.defaultDataSource = factoryResult.defaultSchema;
        defaultSchema = parseDefaultSchema(info);
      } else {
        final TestFactory schemaFactory = (TestFactory) schemaFactoryClazz.newInstance();
        List<QuarkSchema> schemas = schemaFactory.create(info);
        for (QuarkSchema schema : schemas) {
          SchemaPlus schemaPlus = rootSchema.add(schema.getName(), schema);
          schema.setSchemaPlus(schemaPlus);
        }
        for (QuarkSchema schema : schemas) {
          schema.initialize(this);
        }
        if (info.getProperty("defaultSchema") == null) {
          throw new QuarkException(new Throwable("Default schema has to be specified"));
        }
        final ObjectMapper mapper = new ObjectMapper();
        defaultSchema =
              Arrays.asList(mapper.readValue(info.getProperty("defaultSchema"), String[].class));
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | IOException e) {
      throw new QuarkException(e);
    }
  }

  public SchemaPlus getRootSchema() {
    return rootSchema;
  }

  public List<String> getDefaultSchemaPath() {
    return defaultSchema;
  }

  public SchemaPlus getDefaultSchema() {
    SchemaPlus defaultSchemaPlus = this.rootSchema;
    for (String schemaName : this.defaultSchema) {
      defaultSchemaPlus = defaultSchemaPlus.getSubSchema(schemaName);
    }
    return defaultSchemaPlus;
  }

  public CalcitePrepare.Context getPrepareContext() {
    return new CalcitePrepare.Context() {

      @Override
      public JavaTypeFactory getTypeFactory() {
        return typeFactory;
      }

      @Override
      public CalciteSchema getRootSchema() {
        return CalciteSchema.from(rootSchema);
      }

      @Override
      public List<String> getDefaultSchemaPath() {
        return defaultSchema;
      }

      @Override
      public CalciteConnectionConfig config() {
        return cfg;
      }

      @Override
      public CalcitePrepare.SparkHandler spark() {
        return CalcitePrepare.Dummy.getSparkHandler(false);
      }

      @Override
      public DataContext getDataContext() {
        return null;
      }
    };
  }

  public DataSourceSchema getDefaultDataSource() {
    return defaultDataSource;
  }

  public boolean isUnitTestMode() {
    return unitTestMode;
  }
}
