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
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.qubole.quark.QuarkException;

import com.qubole.quark.sql.QueryContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stores definitions of cubes and views. In the future, any other relationships
 * between tables will be stored in the schema. The name of this schema will always
 * be "QUARK_METADATA"
 */

public abstract class MetadataSchema extends QuarkSchema {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataSchema.class);
  public static final String NAME = "QUARK_METADATA";

  protected List<QuarkCube> cubes;
  protected List<QuarkView> views;

  public MetadataSchema() {
    super(NAME);
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return ImmutableMap.of();
  }

  public List<QuarkView> getViews() {
    return views;
  }

  public List<QuarkCube> getCubes() {
    return cubes;
  }

  @Override
  public void initialize(final QueryContext queryContext) throws QuarkException {
    CalciteSchema calciteSchema = CalciteSchema.from(schemaPlus);
    CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
        calciteSchema.root(),
        false,
        queryContext.getDefaultSchemaPath(),
        queryContext.getTypeFactory());

    for (final QuarkView view : this.getViews()) {
      LOG.debug("Adding view " + view.name);

      MaterializationService.TableFactory tableFactory =
          new MaterializationService.TableFactory() {
            @Override
            public Table createTable(CalciteSchema schema,
                                     String viewSql,
                                     List<String> viewSchemaPath) {
              final List<String> tableName = new ArrayList<String>();
              tableName.addAll(view.schema);
              tableName.add(view.table);

              CalciteCatalogReader calciteCatalogReader =
                  new CalciteCatalogReader(schema.root(),
                      false,
                      queryContext.getDefaultSchemaPath(),
                      queryContext.getTypeFactory());
              CalciteSchema viewSchema =
                  calciteCatalogReader.getTable(tableName)
                      .unwrap(CalciteSchema.class);
              assert viewSchema != null;
              CalciteSchema.TableEntry viewTEntry = viewSchema.getTable(
                  view.table,
                  false);
              assert viewTEntry != null;
              Table backupTable = viewTEntry.getTable();
              final RelDataType rowType =
                  backupTable.getRowType(queryContext.getTypeFactory());
              RelOptTableImpl relOptTable = RelOptTableImpl.create(
                  calciteCatalogReader,
                  rowType,
                  backupTable,
                  Schemas.path(viewSchema, view.alias));
              QuarkViewTable table = new QuarkViewTable(view.table,
                  relOptTable,
                  (QuarkTable) backupTable,
                  viewSchema);
              return table;
            }
          };
      MaterializationService.instance().defineMaterialization(calciteSchema,
          null, view.viewSql, view.schema, view.table,
          tableFactory, true, false);
    }

    for (QuarkCube cube : this.getCubes()) {
      CalciteSchema tileSchema = calciteCatalogReader.getTable(cube.tableName)
          .unwrap(CalciteSchema.class);
      assert tileSchema != null;
      CalciteSchema.TableEntry tileTEntry = tileSchema.getTable(
          Util.last(cube.tableName),
          false);
      schemaPlus.add(cube.name,
          cube.build(CalciteSchema.from(queryContext.getRootSchema()),
          (QuarkTable) tileTEntry.getTable()));
    }
    final MaterializationService service = MaterializationService.instance();
    for (CalciteSchema.LatticeEntry e : Schemas.getLatticeEntries(
        CalciteSchema.from(queryContext.getRootSchema()))) {
      final Lattice lattice = e.getLattice();
      for (Lattice.Tile tile : lattice.computeTiles()) {
        final QuarkTile nzTile = (QuarkTile) tile;
        MaterializationService.TableFactory tableFactory =
            new MaterializationService.TableFactory() {
              @Override
              public Table createTable(CalciteSchema schema,
                                       String viewSql,
                                       List<String> viewSchemaPath) {
                assert nzTile.tableName != null;
                CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                    schema.root(),
                    false,
                    queryContext.getDefaultSchemaPath(),
                    queryContext.getTypeFactory());
                CalciteSchema tileSchema = calciteCatalogReader.getTable(nzTile.tableName)
                    .unwrap(CalciteSchema.class);
                assert tileSchema != null;
                CalciteSchema.TableEntry tileTEntry = tileSchema.getTable(
                    Util.last(nzTile.tableName),
                    false);
                assert tileTEntry != null;
                return new QuarkTileTable(nzTile, calciteCatalogReader,
                    tileTEntry.getTable().getRowType(queryContext.getTypeFactory()),
                    Schemas.path(tileSchema, nzTile.alias),
                    (QuarkTable) tileTEntry.getTable());
              }
            };
        service.defineTile(lattice, tile.bitSet(), tile.measures, e.schema,
            true, true, Util.last(nzTile.tableName), tableFactory);
      }
    }
  }

  public static MetadataSchema empty() {
    return new MetadataSchema() {
      @Override
      public List<QuarkView> getViews() {
        return ImmutableList.of();
      }

      @Override
      public List<QuarkCube> getCubes() {
        return ImmutableList.of();
      }
    };
  }
}
