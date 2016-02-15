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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a cube. It stores all the information required to
 * materialize tiles in Calcite planner.
 * QuarkCubes are materialized by {@link MetadataSchema}.
 * QuarkCubes are created by derived classes of {@link MetadataSchema}
 * For e.g., its created from JSON in Quark JDBC driver.
 */
public class QuarkCube {
  /**
   * Abstract class for Measure of a cube
   */
  public static class Measure {
    /**
     * Arguments to the measure.
     * Valid values are:
     * <ul>
     * <li>Not specified: no arguments</li>
     * <li>null: no arguments</li>
     * <li>Empty list: no arguments</li>
     * <li>String: single argument, the name of a lattice column</li>
     * <li>List: multiple arguments, each a column name</li>
     * </ul>
     * <p>Unlike lattice dimensions, measures can not be specified in qualified
     * format, {@code ["table", "column"]}. When you define a lattice, make sure
     * that each column you intend to use as a measure has a unique name within
     * the lattice (using "{@code AS alias}" if necessary).</p>
     */

    public final String agg;
    public final Object args;
    public final String cubeColumn;

    public Measure(String agg, String args, String cubeColumn) {
      this.agg = agg;
      this.args = args.toUpperCase();
      this.cubeColumn = cubeColumn.toUpperCase();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof QuarkCube.Measure) {
        QuarkCube.Measure that = (QuarkCube.Measure) o;
        return (this.agg.equals(that.agg))
            && (this.args.equals(that.args))
            && (this.cubeColumn.equals(that.cubeColumn));
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= agg.hashCode();
      h *= 1000003;
      h ^= args.hashCode();
      h *= 1000003;
      h ^= cubeColumn.hashCode();
      return h;
    }

  }

  /**
   * Abstract value for Dimension of a cube
   */
  public static class Dimension implements Comparable<Dimension> {
    /**
     * Arguments to the measure.
     * Valid values are:
     * <ul>
     * <li>Not specified: no arguments</li>
     * <li>null: no arguments</li>
     * <li>Empty list: no arguments</li>
     * <li>String: single argument, the name of a lattice column</li>
     * <li>List: multiple arguments, each a column name</li>
     * </ul>
     * <p>Unlike lattice dimensions, measures can not be specified in qualified
     * format, {@code ["table", "column"]}. When you define a lattice, make sure
     * that each column you intend to use as a measure has a unique name within
     * the lattice (using "{@code AS alias}" if necessary).</p>
     */

    public final String name;
    public final Object qualifiedCol;
    public final String cubeColumn;
    public final int cubeOrdinal;
    public final String parentId;
    public final Dimension parentDimension;
    private List<Dimension> childrenDimensions;
    private final boolean mandatory;

    public boolean isMandatory() {
      return mandatory;
    }

    private static Object createQualifiedCol(String schemaName,
                                             String tableName,
                                             String columnName) {
      Object qualifiedCol;
      if (schemaName.isEmpty() && tableName.isEmpty()) {
        qualifiedCol = columnName.toUpperCase();
      } else if (schemaName.isEmpty()) {
        qualifiedCol = new ImmutableList.Builder<String>()
            .add(tableName.toUpperCase())
            .add(columnName.toUpperCase()).build();
      } else {
        qualifiedCol = new ImmutableList.Builder<String>()
            .add(schemaName.toUpperCase())
            .add(tableName.toUpperCase())
            .add(columnName.toUpperCase()).build();
      }
      return qualifiedCol;
    }

    protected Dimension(String name, String schemaName, String tableName, String columnName,
        String cubeColumn, int cubeOrdinal, String parentId, Dimension parentDimension,
        List<Dimension> childrenDimensions, boolean mandatory) {
      this(name, createQualifiedCol(schemaName, tableName, columnName), cubeColumn,
          cubeOrdinal, parentId, parentDimension, childrenDimensions, mandatory);
    }

    protected Dimension(String name, Object qualifiedCol, String cubeColumn, int cubeOrdinal,
        String parentId, Dimension parentDimension, List<Dimension> childrenDimensions,
        boolean mandatory) {
      this.name = name;
      this.qualifiedCol = qualifiedCol;
      this.parentId = parentId;
      this.cubeColumn = cubeColumn.toUpperCase();
      this.cubeOrdinal = cubeOrdinal;
      this.parentDimension = parentDimension;
      this.childrenDimensions = childrenDimensions;
      this.mandatory = mandatory;
    }

    public List<Dimension> getChildrenDimensions() {
      return childrenDimensions;
    }

    public void addChildDimension(Dimension child) {
      childrenDimensions.add(child);
    }

    public int compareTo(Dimension dimension) {
      return Utilities.compare(this.cubeOrdinal, dimension.cubeOrdinal);
    }

    @Override
    public final boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof QuarkCube.Dimension) {
        QuarkCube.Dimension that = (QuarkCube.Dimension) o;
        return (this.qualifiedCol.equals(that.qualifiedCol))
            && (this.cubeColumn.equals(that.cubeColumn))
            && (this.cubeOrdinal == that.cubeOrdinal);
      }
      return false;
    }

    @Override
    public final int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= qualifiedCol.hashCode();
      h *= 1000003;
      h ^= cubeColumn.hashCode();
      h *= 1000003;
      h ^= cubeOrdinal;
      return h;
    }

    @Override
    public String toString() {
      return "Dimension{"
          + "qualifiedColumn=" + qualifiedCol + ", "
          + "cubeColumn=" + cubeColumn + ", "
          + "cubeOrdinal=" + cubeOrdinal
          + "}";
    }

    /*public static Builder builder(String name, Object qualifiedCol, String cubeColumn,
                                  int cubeOrdinal) {
      return new Builder(name, qualifiedCol, cubeColumn, cubeOrdinal);
    }*/

    public static Builder builder(String name, String schemaName, String tableName,
        String columnName, String cubeColumn, int cubeOrdinal) {
      return new Builder(name, schemaName, tableName, columnName,
          cubeColumn, cubeOrdinal);
    }

    /**
     * Builder for Dimensions
     */
    public static class Builder {
      public final String name;
      public final String schemaName;
      public final String tableName;
      public final String columnName;
      public final String cubeColumn;
      public final int cubeOrdinal;
      public String parentId = null;
      public Dimension parentDimension = null;
      private List<Dimension> childrenDimensions = new ArrayList<>();
      private boolean mandatory = false;

      Builder(String name, String schemaName, String tableName,
          String columnName, String cubeColumn, int cubeOrdinal) {
        this.name = name;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.cubeColumn = cubeColumn;
        this.cubeOrdinal = cubeOrdinal;
      }

      public Builder setChildrenDimensions(List<Dimension> childrenDimensions) {
        this.childrenDimensions = childrenDimensions;
        return this;
      }

      public Builder setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
        return this;
      }

      public Builder setParentId(String parentId) {
        this.parentId = parentId;
        return this;
      }

      public Builder setParentDimension(Dimension parentDimension) {
        this.parentDimension = parentDimension;
        return this;
      }

      public Dimension build() {
        return new Dimension(name, schemaName, tableName, columnName, cubeColumn,
            cubeOrdinal, parentId, parentDimension, childrenDimensions, mandatory);
      }
    }
  }

  /**
   * Support for groups of dimensions
   */
  public static class Group {
    public final String name;
    public final String dimensionName;

    public Group(String name, String dimensionName) {
      this.name = name;
      this.dimensionName = dimensionName;
    }
  }

  public final String name;
  /**
   * SQL query that defines the lattice.
   * Must be a string or a list of strings (which are concatenated separated
   * by newlines).
   */
  public final Object sql;
  public final List<Measure> measures;
  public final ImmutableSet<Dimension> dimensions;
  public final ImmutableList<String> tableName;
  public final ImmutableList<String> alias;
  public final String groupingColumn;
  public final Set<Set<Dimension>> groups;

  public QuarkCube(String name, Object sql, List<Measure> measures,
                   ImmutableList<Dimension> dimensionList,
                   List<String> tableName, String groupingColumn) {
    this(name, sql, measures, dimensionList, ImmutableList.<Group>of(),
        tableName, groupingColumn, tableName);
  }

  public QuarkCube(String name, Object sql, List<Measure> measures,
                   ImmutableList<Dimension> dimensionList,
                   ImmutableList<Group> groupList,
                   List<String> tableName, String groupingColumn) {
    this(name, sql, measures, dimensionList, groupList, tableName, groupingColumn, tableName);
  }

  public QuarkCube(String name, Object sql, List<Measure> measures,
                   ImmutableList<Dimension> dimensionList,
                   ImmutableList<Group> groupList,
                   List<String> tableName, String groupingColumn,
                   List<String> alias) {
    this.name = name;
    this.sql = sql;
    this.measures = measures;
    this.tableName = toUpperCase(tableName);
    this.groupingColumn = groupingColumn.toUpperCase();
    this.alias = toUpperCase(alias);

    Map<String, Set<Dimension>> groupToDimensionMap = new HashMap<>();
    Map<String, Dimension> idToDimensionMap = new HashMap<>();

    this.buildGroups(dimensionList, groupList, groupToDimensionMap, idToDimensionMap);
    this.groups = new HashSet<>(groupToDimensionMap.values());
    this.dimensions = ImmutableSet.copyOf(
        Ordering.natural().immutableSortedCopy(idToDimensionMap.values()));
  }

  private void buildGroups(ImmutableList<Dimension> dimensions,
                           ImmutableList<Group> groupList,
                           Map<String, Set<Dimension>> groupToDimensionMap,
                           Map<String, Dimension> idToDimensionMap) {

    ImmutableList<Dimension> dimToBeAdded = dimensions;
    //code below makes sure that for heirarichal dimension,
    // parent dimension is created before child
    while (!dimToBeAdded.isEmpty()) {
      ImmutableList.Builder<Dimension> parentJsonDimensionNotCreated =
          new ImmutableList.Builder<>();
      for (Dimension dimension : dimToBeAdded) {
        if (dimension.parentId == null || idToDimensionMap.get(dimension.parentId) != null) {
          // either parent is null or has been already created i.e., added to List builder
          // then create child jsonDimension
          addDimension(groupToDimensionMap, groupList, idToDimensionMap, dimension);
        } else {
          // otherwise, parent is still not created so wait till it is created
          parentJsonDimensionNotCreated.add(dimension);
        }
      }
      // again go through the dimesnions that have not been created yet as their parents
      // were not created yet
      dimToBeAdded = parentJsonDimensionNotCreated.build();
    }
  }

  private void addDimension(Map<String, Set<QuarkCube.Dimension>> groupToDimensionMap,
                            ImmutableList<Group> groupList,
                            Map<String, QuarkCube.Dimension> idToDimensionMap,
                            Dimension dimension) {
    Dimension parentDimension = null;
    if (dimension.parentId != null) {
      parentDimension = idToDimensionMap.get(dimension.parentId);
    }
    final QuarkCube.Dimension element = new QuarkCube.Dimension(dimension.name,
        dimension.qualifiedCol, dimension.cubeColumn, dimension.cubeOrdinal,
        dimension.parentId, parentDimension, dimension.childrenDimensions, dimension.mandatory);

    idToDimensionMap.put(dimension.name, element);
    // Add element as a child of it's parent jsonDimension.
    if (parentDimension != null) {
      parentDimension.addChildDimension(element);
    }

    for (Group group : groupList) {
      if (group.dimensionName.equals(element.name)) {
        Set<QuarkCube.Dimension> dimensionSet = groupToDimensionMap.get(group.name);
        if (dimensionSet == null) {
          dimensionSet = new HashSet<>();
          groupToDimensionMap.put(group.name, dimensionSet);
        }
        dimensionSet.add(element);
      }
    }
  }

  public Lattice build(CalciteSchema calciteSchema, QuarkTable quarkTable) {
    Lattice.Builder latticeBuilder =
        Lattice.builder(calciteSchema, toString(this.sql))
            .auto(false)
            .algorithm(false);

    final ImmutableBiMap<Integer, Integer> dimensionToCubeColumn =
        getDimensionToCubeColumnMap(quarkTable, latticeBuilder);

    validateCubeLatticeFilter(latticeBuilder, dimensionToCubeColumn);

    List<Lattice.Measure> measures = new ArrayList<>();
    for (QuarkCube.Measure nzMeasure : this.measures) {
      final Lattice.Measure measure =
          latticeBuilder.resolveMeasure(nzMeasure.agg, nzMeasure.args);
      QuarkTile.Measure quarkMeasure = new QuarkTile.Measure(measure,
          quarkTable.getFieldOrdinal(nzMeasure.cubeColumn));
      measures.add(quarkMeasure);
    }

    final Set<Set<Dimension>> dimensionSets;
    if (groups == null || groups.isEmpty()) {
      dimensionSets = getDimensionSets(dimensions);
    } else {
      dimensionSets = ImmutableSet.<Set<Dimension>>builder()
          .addAll(groups) //Add all possible groups
          .add(Sets.<Dimension>newHashSet()) //Add an empty set
          .build();
    }

    for (Set<Dimension> set : dimensionSets) {
      List<Lattice.Column> columns = new ArrayList<>();
      List<QuarkTile.Column> cubeColumns = new ArrayList<>();
      ImmutableBitSet.Builder bitSetBuilder = new ImmutableBitSet.Builder();

      for (Dimension dimension : set) {
        final Lattice.Column column = latticeBuilder.resolveColumn(dimension.qualifiedCol);
        QuarkTile.Column quarkColumn = new QuarkTile.Column(column,
            quarkTable.getFieldOrdinal(dimension.cubeColumn));
        columns.add(column);
        cubeColumns.add(quarkColumn);
        bitSetBuilder.set(dimension.cubeOrdinal);
      }

      latticeBuilder.addTile(new QuarkTile(measures, columns, cubeColumns,
          quarkTable.getFieldOrdinal(this.groupingColumn), bitSetBuilder.build(),
          this.tableName, this.alias, dimensionToCubeColumn));
    }
    return latticeBuilder.build();
  }

  private void validateCubeLatticeFilter(Lattice.Builder latticeBuilder,
      ImmutableBiMap<Integer, Integer> dimensionToCubeColumn) {
    if (latticeBuilder.filter != null) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(latticeBuilder.filter);
      ImmutableBitSet dims = ImmutableBitSet.of(dimensionToCubeColumn.keySet());
      if (!dims.contains(rCols)) {
        throw new RuntimeException("Cube filter is only allowed on dimensions");
      }
    }
  }

  private ImmutableBiMap<Integer, Integer> getDimensionToCubeColumnMap(QuarkTable quarkTable,
      Lattice.Builder latticeBuilder) {
    ImmutableBiMap.Builder<Integer, Integer> builder =  ImmutableBiMap.builder();
    for (Dimension dimension : dimensions) {
      final Lattice.Column column = latticeBuilder.resolveColumn(dimension.qualifiedCol);
      builder.put(column.ordinal,
          quarkTable.getFieldOrdinal(dimension.cubeColumn));
    }
    return builder.build();
  }

  public static Set<Set<Dimension>> getDimensionSets(ImmutableSet<Dimension> dimensions) {
    Set<Set<Dimension>> result = Sets.newHashSet();
    result.add(new HashSet<Dimension>());
    for (Dimension d : dimensions) {
      // traverse only the top level dimension i.e., with no parents
      if (d.parentDimension == null) {
        result = cartesian(ImmutableList.of(result,
            getHierarichalSet(d, new AtomicBoolean(false))));
      }
    }
    return result;
  }



  private static Set<Set<Dimension>> getHierarichalSet(Dimension d,
      AtomicBoolean isChildMandatory) {
    if (d.isMandatory()) {
      isChildMandatory.set(true);
    }
    final Set<Dimension> dSet = Sets.newHashSet(d);
    if (d.getChildrenDimensions().isEmpty()) {
      return (d.isMandatory()) ? Sets.<Set<Dimension>>newHashSet(dSet)
          : Sets.newHashSet(dSet, Sets.<Dimension>newHashSet());
    } else {
      ImmutableList.Builder<Set<Set<Dimension>>> cartesianList = ImmutableList.builder();
      cartesianList.add((Set) Sets.<Set<Dimension>>newHashSet(dSet));
      for (Dimension child : d.getChildrenDimensions()) {
        cartesianList.add(getHierarichalSet(child, isChildMandatory));
      }
      Set<Set<Dimension>> result = cartesian(cartesianList.build());
      if (!isChildMandatory.get()) {
        result.add(Sets.newHashSet(new HashSet<Dimension>()));
      }
      return result;
    }
  }

  private ImmutableList<String> toUpperCase(List<String> stringList) {
    ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<>();
    for (String elem : stringList) {
      listBuilder.add(elem.toUpperCase());
    }
    return listBuilder.build();
  }

  /**
   * Converts a string or a list of strings to a string. The list notation
   * is a convenient way of writing long multi-line strings in JSON.
   */
  static String toString(Object o) {
    return o == null ? null
        : o instanceof String ? (String) o
        : concatenate((List) o);
  }

  /**
   * Converts a list of strings into a multi-line string.
   */
  private static String concatenate(List list) {
    final StringBuilder buf = new StringBuilder();
    for (Object o : list) {
      if (!(o instanceof String)) {
        throw new RuntimeException(
            "each element of a string list must be a string; found: " + o);
      }
      buf.append((String) o);
      buf.append("\n");
    }
    return buf.toString();
  }

  public static <E, T extends Set<E>> Set<Set<E>> cartesian(List<Set<T>> list) {
    final Set<List<T>> cartesianSet = Sets.cartesianProduct(list);
    return Sets.newHashSet(Iterables.transform(cartesianSet,
        new Function<List<T>, Set<E>>() {
          public Set<E> apply(List<T> l) {
            return Sets.newHashSet(Iterables.concat(l));
          }
        }
    ));
  }
}
