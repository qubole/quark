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
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;

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
  MaterializePrepare prepare = new MaterializePrepare();
  public void populateMaterializations(CalcitePrepare.Context context,
      RelOptPlanner planner, Prepare.Materialization materialization) {
    prepare.populateMaterializations(context, planner, materialization);

  }

  /**
   * Inherited from {@link CalcitePrepareImpl} to
   * make populateMaterializations accessible
   */
  private class MaterializePrepare extends CalcitePrepareImpl {
    public void populateMaterializations(Context context,
                                  RelOptPlanner planner,
                                  Prepare.Materialization materialization) {
      super.populateMaterializations(context, planner, materialization);
      planner.addMaterialization(
          new RelOptMaterialization(materialization.tableRel,
              materialization.queryRel,
              materialization.getStarTableIdentified()));
    }
  }
}
