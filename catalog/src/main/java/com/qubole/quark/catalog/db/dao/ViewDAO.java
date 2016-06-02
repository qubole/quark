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

package com.qubole.quark.catalog.db.dao;

import com.qubole.quark.catalog.db.mapper.ViewMapper;
import com.qubole.quark.catalog.db.pojo.View;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;

import java.util.List;

/**
 * DAO for {@link View}
 */
@UseStringTemplate3StatementLocator
@RegisterMapper(ViewMapper.class)
public interface ViewDAO {

  @SqlQuery("select p.id, p.name, p.description, p.query, p.cost, p.table_name, p.schema_name, "
      + "p.destination_id, ds.name as destination, p.ds_set_id from data_sources ds join "
      + "partitions p on p.destination_id = ds.id where ds.ds_set_id = :ds_set_id")
  List<View> findByDSSetId(@Bind("ds_set_id")long dsSetId);

  @SqlQuery("select p.id, p.name, p.description, p.query, p.cost, p.table_name, p.schema_name, "
      + "p.destination_id, ds.name as destination, p.ds_set_id from data_sources ds join "
      + "partitions p on p.destination_id = ds.id where p.name like :like_pattern and ds.ds_set_id = :ds_set_id")
  List<View> findLikeName(@Bind("like_pattern") String likePattern, @Bind("ds_set_id")long dsSetId);

  @SqlQuery("select p.id, p.name, p.description, p.query, p.cost, p.table_name, p.schema_name, "
      + "p.destination_id, ds.name as destination, p.ds_set_id from data_sources ds join "
      + "partitions p on p.destination_id = ds.id where p.id = :id and ds.ds_set_id = :ds_set_id")
  View find(@Bind("id")long id, @Bind("ds_set_id") long dsSetId);

  @SqlQuery("select p.id, p.name, p.description, p.query, p.cost, p.table_name, p.schema_name, "
      + "p.destination_id, ds.name as destination, p.ds_set_id from data_sources ds join "
      + "partitions p on p.destination_id = ds.id where p.name = :name and ds.ds_set_id = :ds_set_id")
  View findByName(@Bind("name")String name, @Bind("ds_set_id") long dsSetId);

  @GetGeneratedKeys
  @SqlUpdate("insert into partitions(name, description, query, cost, destination_id, "
      + "schema_name, table_name, ds_set_id) values(:name, :description, :query, :cost, "
      + ":destination_id, :schema_name, :table_name, :ds_set_id)")
  int insert(@Bind("name") String name, @Bind("description") String description,
             @Bind("query") String query, @Bind("cost") long cost,
             @Bind("destination_id") long destinationId, @Bind("schema_name") String schemaName,
             @Bind("table_name") String tableName, @Bind("ds_set_id") long dsSetId);

  @SqlUpdate("update partitions set name = :v.name, description = :v.description, "
      + "query = :v.query, cost = :v.cost, schema_name = :v.schema, "
      + "table_name = :v.table, destination_id = :v.destinationId where id = :v.id and ds_set_id = :ds_set_id")
  int update(@BindBean("v") View view, @Bind("ds_set_id") long dsSetId);

  @SqlUpdate("delete from partitions where name = :name and ds_set_id = :ds_set_id")
  void delete(@Bind("name") String name, @Bind("ds_set_id") long dsSetId);
}
