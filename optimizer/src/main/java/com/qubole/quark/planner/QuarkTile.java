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

package com.qubole.quark.planner;

import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Ordering;


import java.util.List;

/**
 * Represents a specific tile or cell in a {@link QuarkCube}.
 * Physically its a subset of rows in a table in a {@link com.qubole.quark.plugin.DataSource}
 */
public class QuarkTile extends Lattice.Tile {
  /**
   * Measure for Tile
   */
  public static class Measure extends Lattice.Measure {
    public final int ordinal;

    Measure(Lattice.Measure measure, int ordinal) {
      super(measure.agg, measure.args);
      this.ordinal = ordinal;
    }
  }

  /**
   * Column for Tile
   */
  public static class Column implements Comparable<Column> {
    public final int cubeOrdinal;
    public final int ordinal;

    Column(Lattice.Column column, int cubeOrdinal) {
      this.ordinal = column.ordinal;
      this.cubeOrdinal = cubeOrdinal;
    }

    public int compareTo(Column column) {
      return Utilities.compare(ordinal, column.ordinal);
    }
  }

  public final List<String> tableName;
  public final List<Column> cubeColumns;
  public final int groupingColumn;
  public final ImmutableBitSet groupingValue;
  public final List<String> alias;
  public final ImmutableBiMap<Integer, Integer> dimensionToCubeColumn;

  public QuarkTile(List<Lattice.Measure> measures,
                   List<Lattice.Column> dimensions,
                   List<QuarkTile.Column> cubeColumns,
                   int groupingColumn,
                   ImmutableBitSet groupingValue,
                   List<String> tableName, List<String> alias,
                   ImmutableBiMap<Integer, Integer> dimensionToCubeColumn) {
    super(Ordering.natural().immutableSortedCopy(measures),
        Ordering.natural().immutableSortedCopy(dimensions));
    this.tableName = tableName;
    this.cubeColumns = Ordering.natural().immutableSortedCopy(cubeColumns);
    this.groupingColumn = groupingColumn;
    this.groupingValue = groupingValue;
    this.alias = alias;
    this.dimensionToCubeColumn = dimensionToCubeColumn;
  }
}
