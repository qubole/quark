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
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Function;
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
     * <p/>
     * <p>Valid values are:
     * <ul>
     * <li>Not specified: no arguments</li>
     * <li>null: no arguments</li>
     * <li>Empty list: no arguments</li>
     * <li>String: single argument, the name of a lattice column</li>
     * <li>List: multiple arguments, each a column name</li>
     * </ul>
     * <p/>
     * <p>Unlike lattice dimensions, measures can not be specified in qualified
     * format, {@code ["table", "column"]}. When you define a lattice, make sure
     * that each column you intend to use as a measure has a unique name within
     * the lattice (using "{@code AS alias}" if necessary).
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
     * <p/>
     * <p>Valid values are:
     * <ul>
     * <li>Not specified: no arguments</li>
     * <li>null: no arguments</li>
     * <li>Empty list: no arguments</li>
     * <li>String: single argument, the name of a lattice column</li>
     * <li>List: multiple arguments, each a column name</li>
     * </ul>
     * <p/>
     * <p>Unlike lattice dimensions, measures can not be specified in qualified
     * format, {@code ["table", "column"]}. When you define a lattice, make sure
     * that each column you intend to use as a measure has a unique name within
     * the lattice (using "{@code AS alias}" if necessary).
     */

    public final String name;
    public final Object qualifiedCol;
    public final String cubeColumn;
    public final int cubeOrdinal;
    public final String parentId;
    public final Dimension parentDimension;
    private List<Dimension> childrenDimensions;

    public Dimension(Object qualifiedCol, String cubeColumn, int cubeOrdinal) {
      this.qualifiedCol = qualifiedCol;
      this.cubeColumn = cubeColumn;
      this.cubeOrdinal = cubeOrdinal;
      this.name = cubeColumn;
      this.parentId = null;
      this.parentDimension = null;
      this.childrenDimensions = ImmutableList.of();
    }

    public Dimension(String name, String schemaName, String tableName, String columnName,
                     String cubeColumn, int cubeOrdinal, String parent) {
      this(name, schemaName, tableName, columnName, cubeColumn, cubeOrdinal, parent, null,
          new ArrayList<Dimension>());
    }

    public Dimension(String name, String schemaName, String tableName, String columnName,
                     String cubeColumn, int cubeOrdinal, String parentId,
                     Dimension parentDimension, List<Dimension> childrenDimensions) {
      if (schemaName.isEmpty() && tableName.isEmpty()) {
        this.qualifiedCol = columnName.toUpperCase();
      } else if (schemaName.isEmpty()) {
        this.qualifiedCol = new ImmutableList.Builder<String>()
            .add(tableName.toUpperCase())
            .add(columnName.toUpperCase()).build();
      } else {
        this.qualifiedCol = new ImmutableList.Builder<String>()
            .add(schemaName.toUpperCase())
            .add(tableName.toUpperCase())
            .add(columnName.toUpperCase()).build();
      }
      this.name = name;
      this.cubeColumn = cubeColumn.toUpperCase();
      this.cubeOrdinal = cubeOrdinal;
      this.parentDimension = parentDimension;
      this.childrenDimensions = childrenDimensions;
      this.parentId = parentId;
    }

    public Dimension(String name, Object qualifiedCol,
                     String cubeColumn, int cubeOrdinal, String parentId,
                     Dimension parentDimension, List<Dimension> childrenDimensions) {
      this.qualifiedCol = qualifiedCol;
      this.name = name;
      this.cubeColumn = cubeColumn.toUpperCase();
      this.cubeOrdinal = cubeOrdinal;
      this.parentDimension = parentDimension;
      this.childrenDimensions = childrenDimensions;
      this.parentId = parentId;
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
   * <p/>
   * <p>Must be a string or a list of strings (which are concatenated separated
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
        dimension.qualifiedCol, dimension.cubeColumn, dimension.cubeOrdinal, dimension
        .parentId, parentDimension, dimension.childrenDimensions);

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
      dimensionSets = getDimensionSets();
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
          this.tableName, this.alias));
    }
    return latticeBuilder.build();
  }

  private Set<Set<Dimension>> getDimensionSets() {
    Set<Set<Dimension>> result = Sets.newHashSet();
    result.add(new HashSet<Dimension>());
    for (Dimension d : this.dimensions) {
      // traverse only the top level dimension i.e., with no parents
      if (d.parentDimension == null) {
        result = cartesian(ImmutableList.of(result, getHierarichalSet(d)));
      }
    }
    return result;
  }

  private Set<Set<Dimension>> getHierarichalSet(Dimension d) {
    final Set<Dimension> dSet = Sets.newHashSet(d);
    if (d.getChildrenDimensions().isEmpty()) {
      return Sets.newHashSet(dSet, Sets.<Dimension>newHashSet());
    } else {
      ImmutableList.Builder<Set<Set<Dimension>>> cartesianList = ImmutableList.builder();
      cartesianList.add((Set) Sets.<Set<Dimension>>newHashSet(dSet));
      for (Dimension child : d.getChildrenDimensions()) {
        cartesianList.add(getHierarichalSet(child));
      }
      Set<Set<Dimension>> result = cartesian(cartesianList.build());
      result.add(Sets.newHashSet(new HashSet<Dimension>()));
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
