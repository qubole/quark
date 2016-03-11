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

package com.qubole.quark.catalog.db;

import com.google.common.collect.ImmutableList;
import com.qubole.quark.QuarkException;

import com.qubole.quark.catalog.db.dao.CubeDAO;
import com.qubole.quark.catalog.db.dao.DSSetDAO;
import com.qubole.quark.catalog.db.dao.DimensionDAO;
import com.qubole.quark.catalog.db.dao.JdbcSourceDAO;
import com.qubole.quark.catalog.db.dao.MeasureDAO;
import com.qubole.quark.catalog.db.dao.QuboleDbSourceDAO;
import com.qubole.quark.catalog.db.dao.ViewDAO;
import com.qubole.quark.catalog.db.encryption.MysqlAES;
import com.qubole.quark.catalog.db.pojo.Cube;
import com.qubole.quark.catalog.db.pojo.DSSet;
import com.qubole.quark.catalog.db.pojo.DataSource;
import com.qubole.quark.catalog.db.pojo.JdbcSource;
import com.qubole.quark.catalog.db.pojo.QuboleDbSource;
import com.qubole.quark.planner.QuarkFactory;
import com.qubole.quark.planner.QuarkFactoryResult;

import org.flywaydb.core.Flyway;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Sets up and organizes the planner with tables from all federated data sources.
 * The info argument encodes all the required information (described below) as
 * JSON.
 * Consider an example where there are 3 data sources:
 * <ul>
 * <li>CANONICAL</li>
 * <li>CUBES</li>
 * <li>VIEWS</li>
 * </ul>
 * CANONICAL is the default. Each of the data sources have many schemas and
 * tables. This class and its dependents set up such that the tables can be
 * referred as follows:
 * <ul>
 * <li><i>canonical.default.lineitem</i> refers to a table in DEFAULT in CANONICAL.</li>
 * <li><i>canonical.tpch.customers</i> refers to a table in TPCH in CANONICAL.</li>
 * <li><i>lineitem</i> refers to a table in DEFAULT in CANONICAL.</li>
 * <li><i>CUBES.PUBLIC.WEB_RETURNS</i> refers to a table in PUBLIC in CUBES.</li>
 * </ul>
 *
 */
public class SchemaFactory implements QuarkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaFactory.class);

  /**
   * Creates list of QuarkSchema
   *
   * @param info A JSON string which contains database credentials
   * @return
   * @throws QuarkException
   */
  public QuarkFactoryResult create(Properties info) throws QuarkException {
    try {
      MysqlAES mysqlAES = MysqlAES.getInstance();
      mysqlAES.setKey(info.getProperty("encryptionKey"));

      DBI dbi = new DBI(
          info.getProperty("url"),
          info.getProperty("user"),
          info.getProperty("password"));

      Flyway flyway = new Flyway();
      flyway.setDataSource(
          info.getProperty("url"),
          info.getProperty("user"),
          info.getProperty("password"));
      flyway.migrate();

      DSSetDAO dsSetDAO = dbi.onDemand(DSSetDAO.class);
      JdbcSourceDAO jdbcSourceDAO = dbi.onDemand(JdbcSourceDAO.class);
      QuboleDbSourceDAO quboleDbSourceDAO = dbi.onDemand(QuboleDbSourceDAO.class);
      CubeDAO cubeDAO = dbi.onDemand(CubeDAO.class);
      ViewDAO viewDAO = dbi.onDemand(ViewDAO.class);
      MeasureDAO measureDAO = dbi.onDemand(MeasureDAO.class);
      DimensionDAO dimensionDAO = dbi.onDemand(DimensionDAO.class);

      List<DSSet> dsSets = dsSetDAO.findAll();
      long dsSetId = dsSets.get(0).getId();
      long defaultDataSourceId = dsSets.get(0).getDefaultDatasourceId();
      List<JdbcSource> jdbcSources = jdbcSourceDAO.findByDSSetId(dsSetId);
      List<QuboleDbSource> quboleDbSources = quboleDbSourceDAO.findByDSSetId(dsSetId);

      List<DataSource> dataSources = new ArrayList<>();
      dataSources.addAll(jdbcSources);
      dataSources.addAll(quboleDbSources);

      ImmutableList.Builder<com.qubole.quark.planner.DataSourceSchema> schemaList =
          new ImmutableList.Builder<>();

      com.qubole.quark.planner.DataSourceSchema defaultSchema = null;

      for (DataSource dataSource : dataSources) {
        com.qubole.quark.planner.DataSourceSchema dataSourceSchema = new DataSourceSchema(
            dataSource.getProperties(defaultDataSourceId));
        if (dataSource.getId() == defaultDataSourceId) {
          defaultSchema = dataSourceSchema;
        }
        schemaList.add(dataSourceSchema);
      }

      RelSchema relSchema = getRelSchema(viewDAO, cubeDAO, measureDAO, dimensionDAO, dsSetId);

      return new QuarkFactoryResult(schemaList.build(), relSchema, defaultSchema);
    } catch (Exception se) {
      LOG.error(se.getMessage());
      throw new QuarkException(se);
    }
  }

  private RelSchema getRelSchema(ViewDAO viewDAO,
                                 CubeDAO cubeDAO,
                                 MeasureDAO measureDAO,
                                 DimensionDAO dimensionDAO,
                                 long dsSetId) {

    List<RelSchema.DbView> dbViews = viewDAO.findByDSSetId(dsSetId);
    List<Cube> cubes = cubeDAO.findByDSSetId(dsSetId);
    List<RelSchema.DbCube> dbCubes = new ArrayList<RelSchema.DbCube>();

    for (Cube cube: cubes) {
      List<RelSchema.DbMeasure> dbMeasures =
          measureDAO.findByCubeId(cube.getId());
      List<RelSchema.DbDimension> dbDimensions =
          dimensionDAO.findByCubeId(cube.getId());
      List<RelSchema.DbGroup> dbGroups = ImmutableList.of();

      dbCubes.add(new RelSchema.DbCube(cube.getName(), cube.getQuery(), dbMeasures, dbDimensions,
          dbGroups, cube.getDestination(), cube.getSchemaName(), cube.getTableName(),
          cube.getGroupingColumn()));
    }
    return new RelSchema(dbViews, dbCubes);
  }
}
