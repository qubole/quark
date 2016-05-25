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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;

import com.qubole.quark.catalog.db.pojo.DataSource;
import com.qubole.quark.catalog.db.pojo.View;
import com.qubole.quark.executor.QuarkExecutor;
import com.qubole.quark.executor.QuarkExecutorFactory;
import com.qubole.quark.fatjdbc.QuarkConnectionImpl;
import com.qubole.quark.fatjdbc.QuarkJdbcStatement;
import com.qubole.quark.fatjdbc.QuarkMetaResultSet;
import com.qubole.quark.planner.parser.ParserResult;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by amoghm on 3/4/16.
 */
public class PlanExecutor {
  private final Meta.StatementHandle h;
  private final QuarkConnectionImpl connection;
  private final long maxRowCount;
  private final Cache<String, Connection> connectionCache;

  public PlanExecutor(Meta.StatementHandle h,
               QuarkConnectionImpl connection,
               Cache<String, Connection> connectionCache,
               long maxRowCount) {
    this.h = h;
    this.connection = connection;
    this.connectionCache = connectionCache;
    this.maxRowCount = maxRowCount;
  }

  public QuarkMetaResultSet execute(ParserResult parserResult) throws Exception {
    QuarkJdbcStatement stmt = connection.server.getStatement(h);

    QuarkExecutor executor = QuarkExecutorFactory.getQuarkExecutor(parserResult.getKind(),
        connection.parserFactory, connection.getProperties(), connectionCache);
    Object result = executor.execute(parserResult);

    if (result instanceof Integer) {
      // Alter, Create, Drop DDL commands will either return id or 0
      return QuarkMetaResultSet.count(h.connectionId, h.id, ((Integer) result).intValue());
    } else if (result instanceof ArrayList) {
      // Show DDL returns an arraylist
      Class pojoType = getPojoType(parserResult.getParsedSql());

      return getMetaResultSetFromIterator(
          convertToIterator((ArrayList) result, pojoType),
          connection, parserResult, "", connection.server.getStatement(h), h,
          AvaticaStatement.DEFAULT_FETCH_SIZE, pojoType);
    } else if (result instanceof ResultSet) {
      // Querying JdbcDB
      return QuarkMetaResultSet.create(h.connectionId, h.id, (ResultSet) result,
          maxRowCount);
    } else if (result instanceof Iterator) {
      // Querying QuboleDB
      return getMetaResultSetFromIterator((Iterator<Object>) result,
          connection, parserResult, parserResult.getParsedSql(), stmt, h, maxRowCount, null);
    }

    throw new RuntimeException("Cannot handle execution for: " + parserResult.getParsedSql());
  }

  private Class getPojoType(String sql) throws SQLException {
    if (sql.startsWith("SHOW DATASOURCE ")) {
      return DataSource.class;
    } else if (sql.startsWith("SHOW VIEW ")) {
      return View.class;
    }
    throw new SQLException("Unable to determine POJO for: " + sql);
  }

  private Iterator<Object> convertToIterator(List list, Class pojoType) throws SQLException {
    List<Object> resultSet = new ArrayList<>();

    for (int i = 0; i < list.size(); i++) {
      String[] row = getValues(list.get(i), pojoType);
      resultSet.add(row);
    }
    return  resultSet.iterator();
  }

  private String[] getValues(Object object, Class pojoType) throws SQLException {
    if (DataSource.class.equals(pojoType)) {
      return ((DataSource) object).values();
    } else if (View.class.equals(pojoType)) {
      return ((View) object).values();
    }
    throw new SQLException("Unknown object type for: " + pojoType);
  }

  private RelDataType getRowType(Class clazz) throws SQLException {
    if (DataSource.class.equals(clazz)) {
      return getDataSourceRowType();
    } else if (View.class.equals(clazz)) {
      return getViewRowType();
    }

    throw new SQLException("RowType not defined for class: " + clazz);
  }

  private RelDataType getDataSourceRowType() throws SQLException {

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

  private RelDataType getViewRowType() {

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

  private QuarkMetaResultSet getMetaResultSetFromIterator(Iterator<Object> iterator,
                                                  QuarkConnectionImpl connection,
                                                  ParserResult result,
                                                  String sql,
                                                  QuarkJdbcStatement stmt,
                                                  Meta.StatementHandle h,
                                                  long maxRowCount,
                                                  Class clazz) throws SQLException {
    QuarkMetaResultSet metaResultSet;
    final JavaTypeFactory typeFactory =
        connection.getSqlQueryParser().getTypeFactory();
    final RelDataType x;
    switch (result.getKind()) {
      case INSERT:
      case EXPLAIN:
        x = RelOptUtil.createDmlRowType(result.getKind(), typeFactory);
        break;
      case OTHER_DDL:
        x = getRowType(clazz);
        break;
      default:
        x = result.getRelNode().getRowType();
    }
    RelDataType jdbcType = makeStruct(typeFactory, x);
    final List<ColumnMetaData> columns =
        getColumnMetaDataList(typeFactory, x, jdbcType);
    Meta.Signature signature = new Meta.Signature(columns,
        sql,
        new ArrayList<AvaticaParameter>(),
        new HashMap<String, Object>(),
        Meta.CursorFactory.ARRAY,
        Meta.StatementType.SELECT);
    stmt.setSignature(signature);
    stmt.setResultSet(iterator);
    if (signature.statementType.canUpdate()) {
      metaResultSet = QuarkMetaResultSet.count(h.connectionId, h.id,
          ((Number) iterator.next()).intValue());
    } else {
      metaResultSet = QuarkMetaResultSet.create(h.connectionId, h.id,
          iterator, maxRowCount, signature);
    }
    return metaResultSet;
  }

  private List<ColumnMetaData> getColumnMetaDataList(
      JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType) {
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (Ord<RelDataTypeField> pair : Ord.zip(jdbcType.getFieldList())) {
      final RelDataTypeField field = pair.e;
      final RelDataType type = field.getType();
      final RelDataType fieldType =
          x.isStruct() ? x.getFieldList().get(pair.i).getType() : type;
      columns.add(
          metaData(typeFactory, columns.size(), field.getName(), type,
              fieldType, null));
    }
    return columns;
  }

  private ColumnMetaData metaData(JavaTypeFactory typeFactory, int ordinal,
                                  String fieldName, RelDataType type, RelDataType fieldType,
                                  List<String> origins) {
    final ColumnMetaData.AvaticaType avaticaType =
        avaticaType(typeFactory, type, fieldType);
    return new ColumnMetaData(
        ordinal,
        false,
        true,
        false,
        false,
        type.isNullable()
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true,
        type.getPrecision(),
        fieldName,
        origin(origins, 0),
        origin(origins, 2),
        getPrecision(type),
        getScale(type),
        origin(origins, 1),
        null,
        avaticaType,
        true,
        false,
        false,
        avaticaType.columnClassName());
  }

  private ColumnMetaData.AvaticaType avaticaType(JavaTypeFactory typeFactory,
                                                 RelDataType type, RelDataType fieldType) {
    final String typeName = getTypeName(type);
    if (type.getComponentType() != null) {
      final ColumnMetaData.AvaticaType componentType =
          avaticaType(typeFactory, type.getComponentType(), null);
      final Type clazz = typeFactory.getJavaClass(type.getComponentType());
      final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
      assert rep != null;
      return ColumnMetaData.array(componentType, typeName, rep);
    } else {
      final int typeOrdinal = getTypeOrdinal(type);
      switch (typeOrdinal) {
        case Types.STRUCT:
          final List<ColumnMetaData> columns = new ArrayList<>();
          for (RelDataTypeField field : type.getFieldList()) {
            columns.add(
                metaData(typeFactory, field.getIndex(), field.getName(),
                    field.getType(), null, null));
          }
          return ColumnMetaData.struct(columns);
        default:
          final Type clazz =
              typeFactory.getJavaClass(Util.first(fieldType, type));
          final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
          assert rep != null;
          return ColumnMetaData.scalar(typeOrdinal, typeName, rep);
      }
    }
  }
  private static String getTypeName(RelDataType type) {
    SqlTypeName sqlTypeName = type.getSqlTypeName();
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      // We'd rather print "INTEGER" than "JavaType(int)".
      return sqlTypeName.getName();
    }
    switch (sqlTypeName) {
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        // e.g. "INTERVAL_MONTH" or "INTERVAL_YEAR_MONTH"
        return "INTERVAL_"
            + type.getIntervalQualifier().toString().replace(' ', '_');
      default:
        return type.toString(); // e.g. "VARCHAR(10)", "INTEGER ARRAY"
    }
  }

  private static String origin(List<String> origins, int offsetFromEnd) {
    return origins == null || offsetFromEnd >= origins.size()
        ? null
        : origins.get(origins.size() - 1 - offsetFromEnd);
  }

  private int getTypeOrdinal(RelDataType type) {
    return type.getSqlTypeName().getJdbcOrdinal();
  }

  private static int getScale(RelDataType type) {
    return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
        ? 0
        : type.getScale();
  }

  private static int getPrecision(RelDataType type) {
    return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
        ? 0
        : type.getPrecision();
  }

  private static RelDataType makeStruct(
      RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type.isStruct()) {
      return type;
    }
    return typeFactory.builder().add("$0", type).build();
  }
}
