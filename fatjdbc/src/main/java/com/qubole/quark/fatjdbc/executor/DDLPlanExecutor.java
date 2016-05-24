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
package com.qubole.quark.fatjdbc.executor;

import com.qubole.quark.executor.QuarkDDLExecutor;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlAlterQuarkDataSource;
import org.apache.calcite.sql.SqlAlterQuarkView;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreateQuarkDataSource;
import org.apache.calcite.sql.SqlCreateQuarkView;
import org.apache.calcite.sql.SqlDropQuarkDataSource;
import org.apache.calcite.sql.SqlDropQuarkView;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlShowQuark;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import com.qubole.quark.catalog.db.dao.DataSourceDAO;
import com.qubole.quark.catalog.db.dao.JdbcSourceDAO;
import com.qubole.quark.catalog.db.dao.QuboleDbSourceDAO;
import com.qubole.quark.catalog.db.dao.ViewDAO;
import com.qubole.quark.catalog.db.encryption.AESEncrypt;
import com.qubole.quark.catalog.db.encryption.Encrypt;
import com.qubole.quark.catalog.db.encryption.NoopEncrypt;
import com.qubole.quark.catalog.db.pojo.DataSource;
import com.qubole.quark.catalog.db.pojo.JdbcSource;
import com.qubole.quark.catalog.db.pojo.QuboleDbSource;
import com.qubole.quark.catalog.db.pojo.View;
import com.qubole.quark.fatjdbc.QuarkConnectionImpl;
import com.qubole.quark.fatjdbc.QuarkMetaResultSet;
import com.qubole.quark.planner.parser.ParserResult;

import org.skife.jdbi.v2.DBI;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by amoghm on 3/4/16.
 */
public class DDLPlanExecutor extends PlanExecutor {
  Meta.StatementHandle h;
  QuarkConnectionImpl connection;
  DBI dbi = null;

  DDLPlanExecutor(Meta.StatementHandle h, QuarkConnectionImpl connection) {
    this.h = h;
    this.connection = connection;
  }

  public QuarkMetaResultSet execute(ParserResult parserResult)
      throws Exception {

    SqlParser parser = SqlParser.create(result.getParsedSql(),
        SqlParser.configBuilder()
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build());
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException(
          "parse failed: " + e.getMessage(), e);
    }

    QuarkDDLExecutor quarkDDLExecutor = new QuarkDDLExecutor(connection.parserFactory, connection.getProperties());
    Object result = quarkDDLExecutor.execute(parserResult);

    if (result.getClass().equals(Integer.class)) {
      return QuarkMetaResultSet.count(h.connectionId, h.id, ((Integer) result).intValue());
    } else if (result.getClass().equals(ArrayList.class)) {
      return getQuarkMetaResultSetForDDL((SqlShowQuark) sqlNode, parserResult, null);
    }
    throw new RuntimeException("Cannot handle execution for: " + parserResult.getParsedSql());
  }

  private QuarkMetaResultSet getQuarkMetaResultSetForDDL(SqlShowQuark sqlNode,
                                                         ParserResult result,
                                                         List list) throws SQLException {
    String pojoType = sqlNode.getOperator().toString();

    return getMetaResultSetFromIterator(
        convertToIterator(list, pojoType),
        connection, result, "", connection.server.getStatement(h), h,
        AvaticaStatement.DEFAULT_FETCH_SIZE, sqlNode);
  }

  private Iterator<Object> convertToIterator(List list, String pojoType) throws SQLException {
    List<Object> resultSet = new ArrayList<>();

    for (int i = 0; i < list.size(); i++) {
      String[] row = getValues(list.get(i), pojoType);
      resultSet.add(row);
    }
    return  resultSet.iterator();
  }

  private String[] getValues(Object object, String pojoType) throws SQLException {
    switch (pojoType) {
      case "SHOW_DATASOURCE":
        return ((DataSource) object).values();
      case "SHOW_VIEW":
        return ((View) object).values();
      default:
        throw new SQLException("Unknown object type for: " + pojoType);
    }
  }

  @Override
  protected RelDataType getRowType(SqlNode sqlNode) throws SQLException {
    if (sqlNode instanceof SqlShowQuark) {
      if (((SqlShowQuark) sqlNode).getOperator().toString().equalsIgnoreCase("SHOW_DATASOURCE")) {
        return getDataSourceRowType();
      } else if (((SqlShowQuark) sqlNode).getOperator().toString().equalsIgnoreCase("SHOW_VIEW")) {
        return getViewRowType();
      } else {
        throw new SQLException("RowType not defined for sqlnode: " + sqlNode.toString());
      }
    } else {
      throw new SQLException("Operation not supported for sqlnode: " + sqlNode.toString());
    }
  }

  protected RelDataType getDataSourceRowType() throws SQLException {

    List<RelDataTypeField> relDataTypeFields =
        ImmutableList.<RelDataTypeField>of(
            new RelDataTypeFieldImpl("id", 1, getIntegerJavaType()),
            new RelDataTypeFieldImpl("type", 2, getStringJavaType()),
            new RelDataTypeFieldImpl("url", 3, getStringJavaType()),
            new RelDataTypeFieldImpl("name", 4, getStringJavaType()),
            new RelDataTypeFieldImpl("ds_set_id", 5, getIntegerJavaType()),
            new RelDataTypeFieldImpl("datasource_type", 6, getStringJavaType()),
            new RelDataTypeFieldImpl("auth_token", 7, getStringJavaType()),
            new RelDataTypeFieldImpl("dbtap_id", 8, getIntegerJavaType()),
            new RelDataTypeFieldImpl("username", 9, getStringJavaType()),
            new RelDataTypeFieldImpl("password", 10, getStringJavaType()));

    return new RelRecordType(relDataTypeFields);
  }

  protected RelDataType getViewRowType() {

    List<RelDataTypeField> relDataTypeFields =
        ImmutableList.<RelDataTypeField>of(
            new RelDataTypeFieldImpl("id", 1, getIntegerJavaType()),
            new RelDataTypeFieldImpl("name", 2, getStringJavaType()),
            new RelDataTypeFieldImpl("description", 3, getStringJavaType()),
            new RelDataTypeFieldImpl("cost", 4, getIntegerJavaType()),
            new RelDataTypeFieldImpl("query", 5, getStringJavaType()),
            new RelDataTypeFieldImpl("destination_id", 6, getIntegerJavaType()),
            new RelDataTypeFieldImpl("schema_name", 7, getStringJavaType()),
            new RelDataTypeFieldImpl("table_name", 8, getStringJavaType()),
            new RelDataTypeFieldImpl("ds_set_id", 9, getIntegerJavaType()));

    return new RelRecordType(relDataTypeFields);
  }

  private RelDataTypeFactoryImpl.JavaType getIntegerJavaType() {
    RelDataTypeFactoryImpl relDataTypeFactoryImpl = new JavaTypeFactoryImpl();
    return relDataTypeFactoryImpl.new JavaType(Integer.class);
  }

  private RelDataTypeFactoryImpl.JavaType getStringJavaType() {
    RelDataTypeFactoryImpl relDataTypeFactoryImpl = new JavaTypeFactoryImpl();
    return relDataTypeFactoryImpl.new JavaType(String.class,
        !(String.class.isPrimitive()), Util.getDefaultCharset(), null);
  }
}
