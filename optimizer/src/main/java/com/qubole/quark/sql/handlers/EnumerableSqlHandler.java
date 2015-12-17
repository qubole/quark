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

package com.qubole.quark.sql.handlers;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.qubole.quark.sql.SqlHandlerConfig;
import com.qubole.quark.sql.SqlWorker;
/**
 * Created by amoghm on 11/17/15.
 *
 * Handler to get optimized Enumerable Plan for an String sql
 */
public class EnumerableSqlHandler implements SqlHandler<RelNode, String> {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(EnumerableSqlHandler.class);
  protected final SqlHandlerConfig config;

  public EnumerableSqlHandler(SqlHandlerConfig config) {
    this.config = config;
  }

  @Override
  public RelNode convert(String sql) {
    SqlWorker.PlannerProgram plannerProgram =
        SqlWorker.PlannerProgram.EnumerableProgram;
    Planner planner = config.getPlanner();
    try {
      planner.close();
      planner.reset();
      final SqlNode sqlNode = planner.parse(sql);
      final SqlNode validatedNode = planner.validate(sqlNode);
      final RelNode convertedNode = planner.convert(validatedNode);
      return planner.transform(plannerProgram.getIndex(),
          convertedNode.getTraitSet().plus(EnumerableConvention.INSTANCE).simplify(),
          convertedNode);
    } catch (SqlParseException e) {
      LOGGER.error("SqlParsing failed: " + e.getMessage(), e);
      throw new RuntimeException("SqlParsing failed: " + e.getMessage(), e);
    } catch (ValidationException e) {
      LOGGER.error("Sql Validation failed: " + e.getMessage(), e);
      throw new RuntimeException("Sql Validation failed: " + e.getMessage(), e);
    } catch (RelConversionException e) {
      LOGGER.error("Sql to RelNode conversion or transformation failed: "
          + e.getMessage(), e);
      throw new RuntimeException("Sql to RelNode conversion or transformation failed: "
          + e.getMessage(), e);
    }
  }
}
