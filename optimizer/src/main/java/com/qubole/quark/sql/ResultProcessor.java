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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.DataSourceSchema;
import com.qubole.quark.planner.JoinCalcTransposeRule;
import com.qubole.quark.utilities.RelToSqlConverter;

import java.sql.SQLException;

/**
 * Created by amoghm on 4/5/16.
 */
public class ResultProcessor {
  private ResultProcessor() {
  }

  public static String getParsedSql(RelNode relNode, DataSourceSchema dataSource)
      throws QuarkException, SQLException {
    final SqlDialect dialect = dataSource.getDataSource().getSqlDialect();
    return getParsedSql(relNode, dialect);
  }
  public static String getParsedSql(RelNode relNode, SqlDialect dialect) throws SQLException {
    if (dialect.getDatabaseProduct() == SqlDialect.DatabaseProduct.HIVE) {
      final HepProgram program = new HepProgramBuilder()
          .addRuleInstance(JoinCalcTransposeRule.LEFT_CALC)
          .addRuleInstance(JoinCalcTransposeRule.RIGHT_CALC)
          .addRuleInstance(CalcMergeRule.INSTANCE)
          .build();
      final RelOptPlanner planner = relNode.getCluster().getPlanner();
      final HepPlanner hepPlanner =
          new HepPlanner(program, planner.getContext());
      hepPlanner.setRoot(relNode);
      relNode = hepPlanner.findBestExp();
    }
    RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
    RelToSqlConverter.Result res = relToSqlConverter.visitChild(0, relNode);
    SqlNode sqlNode = res.asQuery();
    String result = sqlNode.toSqlString(dialect, false).getSql();
    return result.replace("\n", " ");
  }
}
