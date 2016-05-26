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
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteRootSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;

import com.qubole.quark.fatjdbc.impl.QuarkServer;
import com.qubole.quark.planner.parser.ParserFactory;
import com.qubole.quark.planner.parser.ParserResult;
import com.qubole.quark.planner.parser.SqlQueryParser;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by rajatv on 10/27/15.
 */
public class QuarkConnectionImpl extends AvaticaConnection implements QuarkConnection {
  public final JavaTypeFactory typeFactory;

  public final QuarkServer server = new QuarkServer();
  public final ParserFactory parserFactory = new ParserFactory(info);

  protected QuarkConnectionImpl(QuarkDriver driver, AvaticaFactory factory, String url,
                                Properties info, CalciteRootSchema rootSchema,
                                JavaTypeFactory typeFactory) throws SQLException {
    super(driver, factory, url, info);

    CalciteConnectionConfig cfg = new CalciteConnectionConfigImpl(info);

    if (typeFactory != null) {
      this.typeFactory = typeFactory;
    } else {
      final RelDataTypeSystem typeSystem =
          cfg.typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);
      this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
    }

    this.properties.put(InternalProperty.CASE_SENSITIVE, cfg.caseSensitive());
    this.properties.put(InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing());
    this.properties.put(InternalProperty.QUOTED_CASING, cfg.quotedCasing());
    this.properties.put(InternalProperty.QUOTING, cfg.quoting());
  }


  public CalciteConnectionConfig config() {
    return new CalciteConnectionConfigImpl(info);
  }

  @Override
  public QuarkStatement createStatement(int resultSetType,
                                        int resultSetConcurrency,
                                        int resultSetHoldability) throws SQLException {
    return (QuarkStatement) super.createStatement(resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public AvaticaPreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    AvaticaPreparedStatement preparedStatement = (AvaticaPreparedStatement) super
        .prepareStatement(sql,
            resultSetType, resultSetConcurrency, resultSetHoldability);
    server.addStatement(this, preparedStatement.handle);
    //server.getStatement(preparedStatement.handle).setSignature(signature);
    return preparedStatement;
  }

  public SchemaPlus getRootSchema() {
    try {
      return getSqlQueryParser().getRootSchma();
    } catch (SQLException e) {
      throw new RuntimeException("Resetting the connection failed: "
          + e.getMessage(), e);
    }
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public Properties getProperties() {
    return info;
  }

  // do not make public
  UnregisteredDriver getDriver() {
    return driver;
  }

  public SqlQueryParser getSqlQueryParser() throws SQLException {
    return parserFactory.getSqlQueryParser(info);
  }

  public synchronized ParserResult parse(String sql) throws SQLException {
    return parserFactory.getParser(sql, info).parse(sql);
  }
}
