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

package com.qubole.quark.planner.parser;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.planner.QuarkTile;
import com.qubole.quark.planner.QuarkTileScan;
import com.qubole.quark.planner.QuarkViewScan;
import com.qubole.quark.planner.QuarkViewTable;
import com.qubole.quark.sql.QueryContext;
import com.qubole.quark.sql.SqlWorker;
import com.qubole.quark.utilities.RelToSqlConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Parser to compile Sql statements of type {@link org.apache.calcite.sql.SqlKind#QUERY}
 * or {@link org.apache.calcite.sql.SqlKind#SET_QUERY} to Plan
 * {@link SqlQueryParserResult}
 */
public class SqlQueryParser implements Parser {
  private static final Logger LOG = LoggerFactory.getLogger(SqlQueryParser.class);
  private final QueryContext context;
  private final SqlWorker worker;

  public SqlQueryParser(Properties info) throws QuarkException {
    this.context = new QueryContext(info);
    this.worker = new SqlWorker(this.context);
  }


  private Function<RelNode, Void> printer() {
    return new Function<RelNode, Void>() {
      public Void apply(RelNode relNode) {
        String s = Util.toLinux(RelOptUtil.toString(relNode));
        LOG.info(s);
        return null;
      }
    };
  }

  public SqlParser getSqlParser(String sql) {
    try {
      final CalciteConnectionConfig config = context.getCfg();
      return SqlParser.create(sql,
          SqlParser.configBuilder()
              .setQuotedCasing(config.quotedCasing())
              .setUnquotedCasing(config.unquotedCasing())
              .setQuoting(config.quoting())
              .build());
    } catch (Exception e) {
      return SqlParser.create(sql);
    }
  }

  private RelNode parseInternal(String sql) throws SQLException {
    try {
      //final CalcitePrepare.Context prepareContext = context.getPrepareContext();
      //Class elementType = Object[].class;
      //RelNode relNode = new QuarkPrepare().prepare(prepareContext, sql, elementType, -1);
      RelNode relNode =  this.worker.parse(sql);
      LOG.info("\n" + RelOptUtil.dumpPlan(
          "", relNode, false, SqlExplainLevel.ALL_ATTRIBUTES));
      return relNode;
    } catch (CalciteContextException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  /**
   * Returns sql string for input dialect.
   *
   * @param dbType DataBase whose Dialect is to be used
   * @return Sql string for input DatabaseType's dialect.
   * @throws SQLException
   */
  private String getParsedSql(RelNode relNode, String dbType) throws SQLException {
    SqlDialect dialect = SqlDialect.getProduct(dbType, null).getDialect();
    return getParsedSql(relNode, dialect);
  }

  private String getParsedSql(RelNode relNode, SqlDialect dialect) throws SQLException {
    RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
    RelToSqlConverter.Result res = relToSqlConverter.visitChild(0, relNode);
    SqlNode sqlNode = res.asQuery();
    String result = sqlNode.toSqlString(dialect, false).getSql();
    return result.replace("\n", " ");
  }

  public SqlQueryParserResult parse(String sql) throws SQLException {
    DataSourceSchema dataSource = this.context.getDefaultDataSource();
    final AtomicBoolean foundNonQuarkScan = new AtomicBoolean(false);
    final ImmutableSet.Builder<DataSourceSchema> dsBuilder = new ImmutableSet.Builder<>();
    try {
      final SqlKind kind = getSqlParser(sql).parseQuery().getKind();
      SqlQueryParserResult result = new SqlQueryParserResult(stripNamespace(sql, dataSource),
          dataSource, kind, null, false);
      RelNode relNode = parseInternal(sql);
      if (context.getDefaultDataSource() != null) {
        final RelVisitor relVisitor = new RelVisitor() {
          @Override
          public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof QuarkViewScan) {
              visitQuarkViewScan((QuarkViewScan) node);
            } else if (node instanceof QuarkTileScan) {
              visitQuarkTileScan((QuarkTileScan) node);
            } else if (node instanceof TableScan) {
              visitNonQuarkScan((TableScan) node);
            }
            super.visit(node, ordinal, parent);
          }

          private void visitNonQuarkScan(TableScan node) {
            foundNonQuarkScan.set(true);
            final String schemaName = node.getTable().getQualifiedName().get(0);
            CalciteSchema schema =
                CalciteSchema.from(getRootSchma()).getSubSchema(schemaName, false);
            dsBuilder.addAll(getDrivers(schema));
          }

          private void visitQuarkTileScan(QuarkTileScan node) {
            QuarkTile quarkTile = node.getQuarkTile();
            CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                CalciteSchema.from(getRootSchma()),
                false,
                context.getDefaultSchemaPath(),
                getTypeFactory());
            CalciteSchema tileSchema = calciteCatalogReader.getTable(quarkTile.tableName)
                .unwrap(CalciteSchema.class);
            dsBuilder.addAll(getDrivers(tileSchema));
          }

          private void visitQuarkViewScan(QuarkViewScan node) {
            QuarkTable table = node.getQuarkTable();
            if (table instanceof QuarkViewTable) {
              final CalciteSchema tableSchema = ((QuarkViewTable) table).getBackupTableSchema();
              dsBuilder.addAll(getDrivers(tableSchema));
            }
          }

          private ImmutableSet<DataSourceSchema> getDrivers(CalciteSchema tableSchema) {
            final ImmutableSet.Builder<DataSourceSchema> dsBuilder =
                new ImmutableSet.Builder<>();
            SchemaPlus tableSchemaPlus = tableSchema.plus();
            while (tableSchemaPlus != null) {
              Schema schema = CalciteSchema.from(tableSchemaPlus).schema;
              if (schema instanceof DataSourceSchema) {
                dsBuilder.add((DataSourceSchema) schema);
              }
              tableSchemaPlus = tableSchemaPlus.getParentSchema();
            }
            return dsBuilder.build();
          }

        };

        relVisitor.go(relNode);
      }

      ImmutableSet<DataSourceSchema> dataSources = dsBuilder.build();

      if (!foundNonQuarkScan.get() && dataSources.size() == 1) {
        /**
         * Check if query is completely optimized for a data source
         */
        final DataSourceSchema newDataSource = dataSources.asList().get(0);
        final SqlDialect dialect = newDataSource.getDataSource().getSqlDialect();
        final String parsedSql = getParsedSql(relNode, dialect);
        result = new SqlQueryParserResult(parsedSql, newDataSource, kind, relNode, true);
      } else if (foundNonQuarkScan.get() && dataSources.size() == 1) {
        /**
         * Check if its not optimized
         */
        final DataSourceSchema newDataSource = dataSources.asList().get(0);
        final String stripNamespace = stripNamespace(sql, newDataSource);
        result = new SqlQueryParserResult(stripNamespace, newDataSource, kind, relNode, true);
      } else if (this.context.isUnitTestMode()) {
        String parsedSql =
            getParsedSql(relNode,
                new SqlDialect(SqlDialect.DatabaseProduct.UNKNOWN, "UNKNOWN", null, true));
        result = new SqlQueryParserResult(parsedSql, null, kind, relNode, true);
      } else if (dataSources.size() > 1) {
        /**
         * Check if it's partially optimized, i.e., tablescans of multiple datasources
         * are found in RelNode. We currently donot support multiple datasources.
         */
        throw new SQLException("Federation between data sources is not allowed", "0A001");
      } else if (dataSources.isEmpty()) {
        throw new SQLException("No dataSource found for query", "3D001");
      }
      return result;
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e.getCause());
    }
  }

  /**
   * Parsed Result of the SqlQueryParser
   */
  public class SqlQueryParserResult extends ParserResult {
    private final DataSourceSchema dataSource;

    public SqlQueryParserResult(String parsedSql, DataSourceSchema dataSource,
                                SqlKind kind, RelNode relNode, boolean parseResult) {
      super(parsedSql, kind, relNode, parseResult);
      this.dataSource = dataSource;
    }

    public DataSourceSchema getDataSource() {
      return dataSource;
    }
  }


  /**
   * Visitor to gather used tables
   */
  class TableGatherer extends RelVisitor {
    RelOptTable usedTable;

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof TableScan) {
        //TODO Implement some asserts here ? For e.g. usedTable should be null.
        usedTable = node.getTable();
      }
      super.visit(node, ordinal, parent);
    }

    public RelOptTable run(RelNode node) {
      go(node);
      return usedTable;
    }
  }

  public List<String> getTables(RelNode relNode) {
    final List<String> result = new ArrayList<>();
    final Set<RelOptTable> usedTables = new LinkedHashSet<RelOptTable>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
          usedTables.add(node.getTable());
        }
        super.visit(node, ordinal, parent);
      }
      // CHECKSTYLE: IGNORE 1
    }.go(relNode);
    for (RelOptTable tbl : usedTables) {
      result.add(StringUtils.join(tbl.getQualifiedName(), "."));
    }
    return result;
  }

  public SchemaPlus getRootSchma() {
    return context.getRootSchema();
  }

  public JavaTypeFactory getTypeFactory() {
    return context.getTypeFactory();
  }

  /**
   * Strips the dataSource name from the query
   * @param query
   * @param dataSource
   * @return
   * @throws QuarkException
   */
  private String stripNamespace(final String query,
                                final DataSourceSchema dataSource)
      throws QuarkException {
    String result = query.replace("\n", " ");
    if (dataSource != null) {
      try {
        final SqlParser parser = getSqlParser(query);
        SqlNode node = parser.parseQuery();
        result = stripNamespace(node, dataSource.getName(),
            dataSource.getDataSource().getSqlDialect());
      } catch (Exception e) {
        LOG.warn("Exception while parsing the input query: " + e.getMessage());
      }
    }
    return result;
  }
  /**
   * Strips namespace from identifiers of sql
   *
   * @param node
   * @param namespace
   * @param dialect
   * @return
   */
  private String stripNamespace(final SqlNode node,
                                final String namespace,
                                final SqlDialect dialect) {
    final SqlNode transformedNode = node.accept(
        new SqlShuttle() {
          @Override
          public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() > 1
                && id.names.get(0).toUpperCase().equals(namespace.toUpperCase())) {
              return id.getComponent(1, id.names.size());
            } else {
              return id;
            }
          }
        });
    String result = transformedNode.toSqlString(dialect).toString();
    return result.replace("\n", " ");
  }
}
