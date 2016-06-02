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

import com.qubole.quark.catalog.db.encryption.Encrypt;
import com.qubole.quark.catalog.db.mapper.QuboleDbSourceMapper;
import com.qubole.quark.catalog.db.pojo.QuboleDbSource;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;

import java.sql.SQLException;
import java.util.List;

/**
 * DAO for {@link QuboleDbSource}
 */
@UseStringTemplate3StatementLocator
@RegisterMapper(QuboleDbSourceMapper.class)
public abstract class QuboleDbSourceDAO {
  @SqlQuery("select ds.id, ds.name, ds.type, ds.datasource_type, ds.url, ds.ds_set_id, "
      + "qs.dbtap_id, qs.auth_token from data_sources ds join quboledb_sources qs on ds.id = qs.id "
      + "where ds.ds_set_id = :ds_set_id")
  public abstract List<QuboleDbSource> findByDSSetId(@Bind("ds_set_id") long dsSetId);

  @SqlQuery("select ds.id, ds.name, ds.type, ds.datasource_type, ds.url, ds.ds_set_id, "
      + "qs.dbtap_id, qs.auth_token from data_sources ds join quboledb_sources qs on ds.id = qs.id "
      + "where ds.name like :like_pattern and ds.ds_set_id = :ds_set_id")
  public abstract List<QuboleDbSource> findLikeName(@Bind("like_pattern") String likePattern,
                                                          @Bind("ds_set_id") long dsSetId);

  @SqlQuery("select ds.id, ds.name, ds.type, ds.datasource_type, ds.url, ds.ds_set_id, "
      + "qs.dbtap_id, qs.auth_token from data_sources ds join quboledb_sources qs on ds.id = qs.id "
      + "where ds.id = :id and ds.ds_set_id = :ds_set_id")
  public abstract QuboleDbSource find(@Bind("id") int id, @Bind("ds_set_id") long dsSetId);

  @SqlQuery("select ds.id, ds.name, ds.type, ds.datasource_type, ds.url, ds.ds_set_id, "
      + "qs.dbtap_id, qs.auth_token from data_sources ds join quboledb_sources qs on ds.id = qs.id "
      + "where ds.name = :name and ds.ds_set_id = :ds_set_id")
  public abstract QuboleDbSource findByName(@Bind("name") String name,
                                            @Bind("ds_set_id") long dsSetId);

  @SqlUpdate("insert into quboledb_sources(id, dbtap_id, auth_token) "
      + "values(:id, :dbtap_id, :auth_token)")
  public abstract void insert(@Bind("id") long id, @Bind("dbtap_id") int dbTapId,
      @Bind("auth_token") String authToken);

  @GetGeneratedKeys
  @SqlUpdate("update quboledb_sources set dbtap_id = :d.dbTapId,"
      + " auth_token = :d.authToken where id = :d.id")
  protected abstract int updateQubole(@BindBean("d") QuboleDbSource db);

  @SqlUpdate("delete from quboledb_sources where id = :id")
  public abstract void delete(@Bind("id") long id);

  @Transaction
  public int update(QuboleDbSource db, DataSourceDAO dao, Encrypt encrypt) {
    try {
      db.setUrl(encrypt.convertToDatabaseColumn(db.getUrl()));
      db.setAuthToken(encrypt.convertToDatabaseColumn(db.getAuthToken()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    updateQubole(db);
    return dao.update(db);
  }
}
