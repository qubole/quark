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
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.cache.Cache;

import com.qubole.quark.fatjdbc.QuarkConnectionImpl;
import com.qubole.quark.fatjdbc.QuarkJdbcStatement;
import com.qubole.quark.fatjdbc.QuarkMetaResultSet;
import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.parser.ParserResult;
import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.plugins.Executor;
import com.qubole.quark.plugins.jdbc.EMRDb;
import com.qubole.quark.plugins.jdbc.JdbcDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by amoghm on 3/4/16.
 *
 * Parser for SQL statements that belongs to
 * {@link org.apache.calcite.sql.SqlKind#QUERY} or
 * {@link org.apache.calcite.sql.SqlKind#SET_QUERY}
 */
public class QueryPlanExecutor implements PlanExecutor {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryPlanExecutor.class);

  public static final int QUERY_TIMEOUT = 60;

  private final Meta.StatementHandle h;
  private final QuarkConnectionImpl connection;
  private final Cache<String, Connection> connectionCache;
  private final long maxRowCount;

  public QueryPlanExecutor(Meta.StatementHandle h, QuarkConnectionImpl connection,
                           Cache<String, Connection> connectionCache, long maxRowCount) {
    this.h = h;
    this.connection = connection;
    this.connectionCache = connectionCache;
    this.maxRowCount = maxRowCount;
  }

  public QuarkMetaResultSet execute(ParserResult result) throws Exception {
    QuarkMetaResultSet metaResultSet = null;
    String sql = result.getParsedSql();
    QuarkJdbcStatement stmt = connection.server.getStatement(h);
    final DataSourceSchema dataSourceSchema =
        ((SqlQueryParser.SqlQueryParserResult) result).getDataSource();
    if (dataSourceSchema != null) {
      Connection conn;
      final String id = dataSourceSchema.getName();
      Iterator<Object> iterator = null;
      Executor executor = (Executor) dataSourceSchema.getDataSource();
      String parsedSql = result.getParsedSql();
      LOG.info("Execute query[" + parsedSql + "]");
      if (executor instanceof JdbcDB) {
        conn = getExecutorConnection(id, executor);
        Statement statement = conn.createStatement();
        try {
          statement.setQueryTimeout(QUERY_TIMEOUT);
        } catch (Exception e) {
          LOG.warn("Couldnot set Query Timeout to " + QUERY_TIMEOUT + " seconds", e);
        }
        ResultSet resultSet = statement.executeQuery(parsedSql);
        metaResultSet = QuarkMetaResultSet.create(h.connectionId, h.id, resultSet,
            maxRowCount);
      } else {
        iterator = executor.executeQuery(parsedSql);
        final JavaTypeFactory typeFactory =
            connection.getSqlQueryParser().getTypeFactory();
        final RelDataType x;
        switch (result.getKind()) {
          case INSERT:
          case EXPLAIN:
            x = RelOptUtil.createDmlRowType(result.getKind(), typeFactory);
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
      }
    }
    return metaResultSet;
  }

  private Connection getExecutorConnection(String id, Executor executor)
      throws SQLException, ClassNotFoundException {
    Connection conn;
    if (executor instanceof EMRDb) {
      if (this.connectionCache.asMap().containsKey(id)) {
        conn = this.connectionCache.getIfPresent(id);
        if (conn.isClosed()) {
          conn = ((EMRDb) executor).getConnectionExec();
          this.connectionCache.put(id, conn);
        }
      } else {
        conn = ((EMRDb) executor).getConnectionExec();
        this.connectionCache.put(id, conn);
      }
    } else {
      if (this.connectionCache.asMap().containsKey(id)) {
        conn = this.connectionCache.getIfPresent(id);
        if (conn.isClosed()) {
          conn = ((JdbcDB) executor).getConnection();
          this.connectionCache.put(id, conn);
        }
      } else {
        conn = ((JdbcDB) executor).getConnection();
        this.connectionCache.put(id, conn);
      }
    }
    return conn;
  }

  private static RelDataType makeStruct(
      RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type.isStruct()) {
      return type;
    }
    return typeFactory.builder().add("$0", type).build();
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
}
