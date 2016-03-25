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
* return back after optimizing.
*/
package com.qubole.quark.sql;

import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.QuarkMaterializeCluster;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Materializer;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.fun.HiveSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

import com.google.common.collect.ImmutableList;

import com.qubole.quark.planner.RuleSets;
import com.qubole.quark.sql.handlers.EnumerableSqlHandler;
import com.qubole.quark.sql.handlers.SqlHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Default SqlQueryParser and planner for String sql.
 */
public class SqlWorker {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(SqlWorker.class);

  private final Planner planner;

  /**
   * Enum for indexes to Program arrays
   * that are set in FrameworkConfig.
   * Will be used by Handlers
   */
  public enum PlannerProgram {
    EnumerableProgram(0);

    int index;

    PlannerProgram(int index) {
      this.index = index;
    }

    public int getIndex() {
      return index;
    }
  }

  private final QueryContext context;
  private final CalciteCatalogReader catalogReader;
  private final QuarkMaterializeCluster.RelOptPlannerHolder plannerHolder;

  public SqlWorker(QueryContext context) {
    this.context = context;
    this.catalogReader = createCatalogReader(context);
    this.plannerHolder = new QuarkMaterializeCluster.RelOptPlannerHolder(null);
    this.planner = buildPlanner(context);
  }

  private CalciteCatalogReader createCatalogReader(QueryContext context) {
    return new CalciteCatalogReader(
        CalciteSchema.from(context.getRootSchema()),
        false,
        context.getDefaultSchemaPath(),
        context.getTypeFactory());
  }

  private Planner buildPlanner(QueryContext context) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    final ChainedSqlOperatorTable opTab =
        new ChainedSqlOperatorTable(
            ImmutableList.of(SqlStdOperatorTable.instance(),
                HiveSqlOperatorTable.instance(), catalogReader));
    FrameworkConfig config = Frameworks.newConfigBuilder() //
        .parserConfig(SqlParser.configBuilder()
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.TO_UPPER)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build()) //
        .defaultSchema(context.getDefaultSchema()) //
        .operatorTable(opTab) //
        .traitDefs(traitDefs) //
        .convertletTable(StandardConvertletTable.INSTANCE)//
        .programs(getPrograms()) //
        .typeSystem(RelDataTypeSystem.DEFAULT) //
        .build();
    return Frameworks.getPlanner(config);
  }

  private RuleSet[] getRules() {
    RuleSet defaultRule = RuleSets.getDefaultRuleSet();
    RuleSet[] allRules = new RuleSet[] {defaultRule};
    return allRules;
  }

  private List<Program> getPrograms() {
    ImmutableList.Builder<Program> builder
        = ImmutableList.builder();
    for (RuleSet ruleSet: getRules()) {
      builder.add(Programs.sequence(
          new EnumerableProgram(ruleSet, this.context, this.plannerHolder),
          Programs.CALC_PROGRAM));
    }
    return builder.build();
  }

  /**
   * Program to convert LogicalRelNode to Optimized EnumerableRel
   */
  public class EnumerableProgram implements Program {
    final RuleSet ruleSet;
    final QueryContext context;
    final MaterializationService materializationService
        = MaterializationService.instance();
    final QuarkMaterializeCluster.RelOptPlannerHolder plannerHolder;
    List<Prepare.Materialization> materializations = null;
    private EnumerableProgram(RuleSet ruleSet, QueryContext context,
                              QuarkMaterializeCluster.RelOptPlannerHolder holder) {
      this.ruleSet = ruleSet;
      this.context = context;
      this.plannerHolder = holder;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
                       RelTraitSet requiredOutputTraits) {
      planner.clear();

      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
      //((VolcanoPlanner) planner).registerAbstractRelationalRules();

      RelOptUtil.registerAbstractRels(planner);
      for (RelOptRule rule : ruleSet) {
        planner.addRule(rule);
      }

      planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
      planner.addRule(ProjectTableScanRule.INSTANCE);
      planner.addRule(ProjectTableScanRule.INTERPRETER);
      planner.addRule(EnumerableInterpreterRule.INSTANCE);

      final CalciteSchema rootSchema = CalciteSchema.from(context.getRootSchema());
      planner.setExecutor(new RexExecutorImpl(null));
      planner.setRoot(rel);

      MaterializationService.setThreadLocal(materializationService);
      plannerHolder.setPlanner(planner);
      populateMaterializationsAndLattice(plannerHolder, rootSchema);
      if (!rel.getTraitSet().equals(requiredOutputTraits)) {
        rel = planner.changeTraits(rel, requiredOutputTraits);
        planner.setRoot(rel);
      }

      RelOptPlanner planner2 = planner.chooseDelegate();
      return planner2.findBestExp();
    }

    private void populateMaterializationsAndLattice(
        QuarkMaterializeCluster.RelOptPlannerHolder plannerHolder,
        CalciteSchema rootSchema) {
      if (materializations == null) {
        materializations =
            MaterializationService.instance().query(rootSchema);
      }
      Materializer materializer = new Materializer(materializations);

      materializer.populateMaterializations(context.getPrepareContext(), plannerHolder);

      List<CalciteSchema.LatticeEntry> lattices = Schemas.getLatticeEntries(rootSchema);

      for (CalciteSchema.LatticeEntry lattice : lattices) {
        final CalciteSchema.TableEntry starTable = lattice.getStarTable();
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final RelOptTableImpl starRelOptTable =
            RelOptTableImpl.create(catalogReader,
                starTable.getTable().getRowType(typeFactory), starTable, null);
        plannerHolder.getPlanner().addLattice(
            new RelOptLattice(lattice.getLattice(), starRelOptTable));
      }
    }
  }

  /**
   * Will convert String sql to optimized Enumerable Node.
   * @param sql
   * @return Optimized EnumerableNode
   */
  public RelNode parse(String sql) {
    SqlHandlerConfig config = new SqlHandlerConfig(planner);
    SqlHandler<RelNode, String> handler =  new EnumerableSqlHandler(config);
    return handler.convert(sql);
  }
}
