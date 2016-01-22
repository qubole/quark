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

package org.apache.calcite.prepare;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.QuarkMaterializeCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rex.RexBuilder;

import java.util.List;

/**
 * Created by amoghm on 11/19/15.
 *
 * 1. Populates a materialization record, converting a table path
 * (essentially a list of strings, like ["hr", "sales"]) into a table object
 * that can be used in the planning process.
 *
 * 2. Adds to the planner
 */
public class Materializer {
  private List<Prepare.Materialization> materializations;
  public Materializer(List<Prepare.Materialization> materializations) {
    this.materializations = materializations;
  }

  public void populateMaterializations(CalcitePrepare.Context context,
                                       QuarkMaterializeCluster.RelOptPlannerHolder holder) {
    MaterializePrepare prepare = new MaterializePrepare(holder);
    for (Prepare.Materialization materialization : materializations) {
      if (materialization.queryRel == null || materialization.tableRel == null) {
        prepare.populateMaterializations(context, materialization);
      }
      holder.getPlanner().addMaterialization(
          new RelOptMaterialization(materialization.tableRel,
              materialization.queryRel,
              materialization.getStarTableIdentified()));
    }
  }

  /**
   * Inherited from {@link CalcitePrepareImpl} to
   * make populateMaterializations accessible
   */
  private class MaterializePrepare extends CalcitePrepareImpl {
    QuarkMaterializeCluster.RelOptPlannerHolder holderPlanner;
    MaterializePrepare(QuarkMaterializeCluster.RelOptPlannerHolder holderPlanner) {
      this.holderPlanner = holderPlanner;
    }
    /** Factory method for cluster. */
    @Override
    protected RelOptCluster createCluster(RelOptPlanner planner,
                                          RexBuilder rexBuilder) {
      return QuarkMaterializeCluster.create(this.holderPlanner, rexBuilder);
    }
    public void populateMaterializations(Context context,
                                         Prepare.Materialization materialization) {
      super.populateMaterializations(context, holderPlanner.getPlanner(), materialization);
    }
  }
}
