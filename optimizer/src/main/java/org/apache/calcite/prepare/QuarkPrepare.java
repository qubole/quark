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

/*
* This is the entry point to Calcite with necessary logic to
* return back after optimizing, by cutting route to execute it.
* */
package org.apache.calcite.prepare;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableBindable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.qubole.quark.planner.AggStarRule;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rajatv on 3/23/15.
 */
public class QuarkPrepare extends CalcitePrepareImpl {
  private static final Set<String> SIMPLE_SQLS =
      ImmutableSet.of(
          "SELECT 1",
          "select 1",
          "SELECT 1 FROM DUAL",
          "select 1 from dual",
          "values 1",
          "VALUES 1");

  private static final List<RelOptRule> ENUMERABLE_RULES =
      ImmutableList.of(
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

  private static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          AggStarRule.INSTANCE,
          AggStarRule.INSTANCE2,
          TableScanRule.INSTANCE,
          COMMUTE
              ? JoinAssociateRule.INSTANCE
              : ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE);

  private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ReduceExpressionsRule.CALC_INSTANCE,
          ReduceExpressionsRule.JOIN_INSTANCE,
          ValuesReduceRule.FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_INSTANCE);

  public RelNode prepare(
      Context context,
      String sql,
      Type elementType,
      int maxRowCount) {
    return prepareRel_(context, sql, elementType, maxRowCount);
  }

  RelNode prepareRel_(
      Context context,
      String sql,
      Type elementType,
      int maxRowCount) {
    if (SIMPLE_SQLS.contains(sql)) {
      return null;
    }
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            context.getRootSchema(),
            context.config().caseSensitive(),
            context.getDefaultSchemaPath(),
            typeFactory);
    final RelOptPlanner planner = createPlanner(context);
    if (planner == null) {
      throw new AssertionError("factory returned null planner");
    }

    return prepareRel2_(context, sql, elementType, maxRowCount,
        catalogReader, planner);
  }

  RelNode prepareRel2_(
      Context context,
      String sql,
      Type elementType,
      int maxRowCount,
      CalciteCatalogReader catalogReader,
      RelOptPlanner planner) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final EnumerableRel.Prefer prefer;
    if (elementType == Object[].class) {
      prefer = EnumerableRel.Prefer.ARRAY;
    } else {
      prefer = EnumerableRel.Prefer.CUSTOM;
    }
    final CalcitePreparingStmt preparingStmt =
        new CalcitePreparingStmt(
            this,
            context,
            catalogReader,
            typeFactory,
            context.getRootSchema(),
            prefer,
            planner,
            EnumerableConvention.INSTANCE);
//                : EnumerableConvention.INSTANCE);

    final Prepare.PreparedResult preparedResult;
    assert sql != null;
    final CalciteConnectionConfig config = context.config();
    SqlParser parser = SqlParser.create(sql,
        SqlParser.configBuilder()
            .setQuotedCasing(config.quotedCasing())
            .setUnquotedCasing(config.unquotedCasing())
            .setQuoting(config.quoting())
            .build());
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException(
          "parse failed: " + e.getMessage(), e);
    }

    Hook.PARSE_TREE.run(new Object[]{sql, sqlNode});

    final CalciteSchema rootSchema = context.getRootSchema();
    final ChainedSqlOperatorTable opTab =
        new ChainedSqlOperatorTable(
            ImmutableList.of(SqlStdOperatorTable.instance(), catalogReader));
    final SqlValidator validator =
        new CalciteSqlValidator(opTab, catalogReader, typeFactory);
    validator.setIdentifierExpansion(true);

    final List<org.apache.calcite.prepare.Prepare.Materialization> materializations =
        config.materializationsEnabled()
            ? MaterializationService.instance().query(rootSchema)
            : ImmutableList.<org.apache.calcite.prepare.Prepare.Materialization>of();
    for (org.apache.calcite.prepare.Prepare.Materialization materialization : materializations) {
      populateMaterializations(context, planner, materialization);
    }
    final List<CalciteSchema.LatticeEntry> lattices =
        Schemas.getLatticeEntries(rootSchema);
    preparedResult = preparingStmt.prepareSql(
        sqlNode, Object.class, validator, true, materializations, lattices);
    return ((Prepare.PreparedResultImpl) preparedResult).getRootRel();
  }

  /**
   * Creates a query planner and initializes it with a default set of
   * rules.
   */
  protected RelOptPlanner createPlanner(
      final CalcitePrepare.Context prepareContext,
      org.apache.calcite.plan.Context externalContext,
      RelOptCostFactory costFactory) {
    if (externalContext == null) {
      externalContext = Contexts.of(prepareContext.config());
    }
    final VolcanoPlanner planner =
        new VolcanoPlanner(costFactory, externalContext);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.registerAbstractRelationalRules();

    RelOptUtil.registerAbstractRels(planner);
    for (RelOptRule rule : DEFAULT_RULES) {
      planner.addRule(rule);
    }

    if (ENABLE_BINDABLE) {
      for (RelOptRule rule : Bindables.RULES) {
        planner.addRule(rule);
      }
    }
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
    planner.addRule(ProjectTableScanRule.INSTANCE);
    planner.addRule(ProjectTableScanRule.INTERPRETER);

    if (ENABLE_ENUMERABLE) {
      for (RelOptRule rule : ENUMERABLE_RULES) {
        planner.addRule(rule);
      }
      planner.addRule(EnumerableInterpreterRule.INSTANCE);
    }

    if (ENABLE_BINDABLE && ENABLE_ENUMERABLE) {
      planner.addRule(
          EnumerableBindable.EnumerableToBindableConverterRule.INSTANCE);
    }
    return planner;
  }

  private SqlValidator createSqlValidator(CalciteCatalogReader catalogReader,
                                          JavaTypeFactory typeFactory) {
    final SqlOperatorTable opTab =
        ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance(), catalogReader);
    return new CalciteSqlValidator(opTab, catalogReader, typeFactory);
  }

  /**
   * Holds state for the process of preparing a SQL statement.
   */
  static class CalcitePreparingStmt extends Prepare
      implements RelOptTable.ViewExpander {
    protected final RelOptPlanner planner;
    protected final RexBuilder rexBuilder;
    protected final QuarkPrepare prepare;
    protected final CalciteSchema schema;
    protected final RelDataTypeFactory typeFactory;
    private final EnumerableRel.Prefer prefer;
    private final Map<String, Object> internalParameters =
        Maps.newLinkedHashMap();
    private int expansionDepth;
    private SqlValidator sqlValidator;

    CalcitePreparingStmt(QuarkPrepare prepare,
                         Context context,
                         CatalogReader catalogReader,
                         RelDataTypeFactory typeFactory,
                         CalciteSchema schema,
                         EnumerableRel.Prefer prefer,
                         RelOptPlanner planner,
                         Convention resultConvention) {
      super(context, catalogReader, resultConvention);
      this.prepare = prepare;
      this.schema = schema;
      this.prefer = prefer;
      this.planner = planner;
      this.typeFactory = typeFactory;
      this.rexBuilder = new RexBuilder(typeFactory);
    }

    @Override
    protected void init(Class runtimeContextClass) {
    }

    /**
     * An {@code EXPLAIN} statement, prepared and ready to execute.
     */
    private static class CalcitePreparedExplain extends Prepare.PreparedExplain {
      CalcitePreparedExplain(
          RelDataType resultType,
          RelDataType parameterRowType,
          RelRoot root,
          boolean explainAsXml,
          SqlExplainLevel detailLevel) {
        super(resultType, parameterRowType, root, explainAsXml, detailLevel);
      }

      public Bindable getBindable() {
        final String explanation = getCode();
        return new Bindable() {
          public Enumerable bind(DataContext dataContext) {
            return Linq4j.singletonEnumerable(explanation);
          }
        };
      }
    }

    public PreparedResult prepareQueryable(
        final Queryable queryable,
        RelDataType resultType) {
      return prepare_(
          new Supplier<RelNode>() {
            public RelNode get() {
              final RelOptCluster cluster =
                  prepare.createCluster(planner, rexBuilder);
              return new LixToRelTranslator(cluster, CalcitePreparingStmt.this)
                  .translate(queryable);
            }
          }, resultType);
    }

    public PreparedResult prepareRel(final RelNode rel) {
      return prepare_(
          new Supplier<RelNode>() {
            public RelNode get() {
              return rel;
            }
          }, rel.getRowType());
    }

    private static RelDataType makeStruct(
        RelDataTypeFactory typeFactory,
        RelDataType type) {
      if (type.isStruct()) {
        return type;
      }
      return typeFactory.builder().add("$0", type).build();
    }

    private PreparedResult prepare_(Supplier<RelNode> fn,
                                    RelDataType resultType) {
      queryString = null;
      Class runtimeContextClass = Object.class;
      init(runtimeContextClass);

      final RelNode rel = fn.get();
      final RelDataType rowType = rel.getRowType();
      final List<Pair<Integer, String>> fields =
          Pair.zip(ImmutableIntList.identity(rowType.getFieldCount()),
              rowType.getFieldNames());
      final RelCollation collation =
          rel instanceof Sort
              ? ((Sort) rel).collation
              : RelCollations.EMPTY;
      RelRoot root = new RelRoot(rel, resultType, SqlKind.SELECT, fields,
          collation);

      if (timingTracer != null) {
        timingTracer.traceTime("end sql2rel");
      }

      final RelDataType jdbcType =
          makeStruct(rexBuilder.getTypeFactory(), resultType);
      fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null);
      parameterRowType = rexBuilder.getTypeFactory().builder().build();

      // Structured type flattening, view expansion, and plugging in
      // physical storage.
      root = root.withRel(flattenTypes(root.rel, true));

      // Trim unused fields.
      root = trimUnusedFields(root);

      final List<Materialization> materializations = ImmutableList.of();
      final List<CalciteSchema.LatticeEntry> lattices = ImmutableList.of();
      root = optimize(root, materializations, lattices);

      if (timingTracer != null) {
        timingTracer.traceTime("end optimization");
      }

      return implement(root);
    }

    @Override
    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        CatalogReader catalogReader) {
      final RelOptCluster cluster = prepare.createCluster(planner, rexBuilder);
      SqlToRelConverter sqlToRelConverter =
          new SqlToRelConverter(this, validator, catalogReader, cluster,
              StandardConvertletTable.INSTANCE);
      sqlToRelConverter.setTrimUnusedFields(true);
      return sqlToRelConverter;
    }

    @Override
    public RelNode flattenTypes(
        RelNode rootRel,
        boolean restructure) {
      final SparkHandler spark = context.spark();
      if (spark.enabled()) {
        return spark.flattenTypes(planner, rootRel, restructure);
      }
      return rootRel;
    }

    @Override
    protected RelNode decorrelate(SqlToRelConverter sqlToRelConverter,
                                  SqlNode query, RelNode rootRel) {
      return sqlToRelConverter.decorrelate(query, rootRel);
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString,
                              List<String> schemaPath) {
      expansionDepth++;

      SqlParser parser = prepare.createParser(queryString);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("parse failed", e);
      }
      // View may have different schema path than current connection.
      final CatalogReader catalogReader =
          this.catalogReader.withSchemaPath(schemaPath);
      SqlValidator validator = createSqlValidator(catalogReader);
      SqlNode sqlNode1 = validator.validate(sqlNode);

      SqlToRelConverter sqlToRelConverter =
          getSqlToRelConverter(validator, catalogReader);
      RelRoot root =
          sqlToRelConverter.convertQuery(sqlNode1, true, false);

      --expansionDepth;
      return root;
    }

    private SqlValidator createSqlValidator(CatalogReader catalogReader) {
      return prepare.createSqlValidator(
          (CalciteCatalogReader) catalogReader,
          (JavaTypeFactory) typeFactory);
    }

    @Override
    protected SqlValidator getSqlValidator() {
      if (sqlValidator == null) {
        sqlValidator = createSqlValidator(catalogReader);
      }
      return sqlValidator;
    }

    @Override
    protected PreparedResult createPreparedExplanation(
        RelDataType resultType,
        RelDataType parameterRowType,
        RelRoot root,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      return new CalcitePreparedExplain(
          resultType, parameterRowType, root, explainAsXml, detailLevel);
    }

    @Override
    protected PreparedResult implement(RelRoot root) {
      RelDataType resultType = root.rel.getRowType();
      boolean isDml = root.kind.belongsTo(SqlKind.DML);
      final Bindable bindable;
//      if (resultConvention == BindableConvention.INSTANCE) {
      bindable = Interpreters.bindable(root.rel);
//      } else {
//        EnumerableRel enumerable = (EnumerableRel) root.rel;
//        if (!root.isRefTrivial()) {
//          final List<RexNode> projects = new ArrayList<>();
//          final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
//          for (int field : Pair.left(root.fields)) {
//            projects.add(rexBuilder.makeInputRef(enumerable, field));
//          }
//          RexProgram program = RexProgram.create(enumerable.getRowType(),
//            projects, null, root.validatedRowType, rexBuilder);
//          enumerable = EnumerableCalc.create(enumerable, program);
//        }
//
//        bindable = EnumerableInterpretable.toBindable(internalParameters,
//          context.spark(), enumerable, prefer);
//      }

      if (timingTracer != null) {
        timingTracer.traceTime("end codegen");
      }

      if (timingTracer != null) {
        timingTracer.traceTime("end compilation");
      }

      return new PreparedResultImpl(
          resultType,
          parameterRowType,
          fieldOrigins,
          root.collation.getFieldCollations().isEmpty()
              ? ImmutableList.<RelCollation>of()
              : ImmutableList.of(root.collation),
          root.rel,
          mapTableModOp(isDml, root.kind),
          isDml) {
        public String getCode() {
          throw new UnsupportedOperationException();
        }

        public Bindable getBindable() {
          return bindable;
        }

        public Type getElementType() {
          return ((Typed) bindable).getElementType();
        }
      };
    }
  }
}
