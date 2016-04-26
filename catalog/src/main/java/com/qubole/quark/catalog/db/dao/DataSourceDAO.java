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
import com.qubole.quark.catalog.db.mapper.DataSourceMapper;
import com.qubole.quark.catalog.db.pojo.DataSource;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;

import java.sql.SQLException;
import java.util.List;

/**
 * DAO for {@link DataSource}
 */
@RegisterMapper(DataSourceMapper.class)
public abstract class DataSourceDAO {
  @SqlQuery("select id, name, type, datasource_type, url, ds_set_id from data_sources "
      + "where ds_set_id = :ds_set_id")
  public abstract List<DataSource> findByDSSetId(@Bind("ds_set_id") long dsSetId);

  @GetGeneratedKeys
  @SqlUpdate("insert into data_sources(name, type, url, ds_set_id, datasource_type) "
      + "values(:name, :type, :url, :ds_set_id, :datasource_type)")
  protected abstract int insert(@Bind("name") String name, @Bind("type") String type,
      @Bind("url") String url, @Bind("ds_set_id") long dsSetId,
      @Bind("datasource_type") String dataSourcetype);

  @GetGeneratedKeys
  @SqlUpdate("update data_sources set name = :d.name,"
      + " type = :d.type, datasource_type = :d.datasourceType,"
      + " url = :d.url, ds_set_id = :d.dsSetId where id = :d.id")
  public abstract int update(@BindBean("d") DataSource ds);

  @SqlUpdate("delete from data_sources where id = :id")
  public abstract void delete(@Bind("id") int id);

  public int insertJDBC(String name, String type, String url, long dsSetId,
                        String dataSourcetype, JdbcSourceDAO jdbcDao, String username,
                        String password, Encrypt encrypt) {
    try {
      url = encrypt.convertToDatabaseColumn(url);
      username = encrypt.convertToDatabaseColumn(username);
      password = encrypt.convertToDatabaseColumn(password);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    int id = insert(name, type, url, dsSetId, dataSourcetype);
    jdbcDao.insert(id, username, password);
    return id;
  }

  @Transaction
  public int insertQuboleDB(String name, String type, String url, long dsSetId,
      String dataSourcetype, QuboleDbSourceDAO quboleDao, int dbTapId, String authToken,
      Encrypt encrypt) {
    try {
      url = encrypt.convertToDatabaseColumn(url);
      authToken = encrypt.convertToDatabaseColumn(authToken);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    int id = insert(name, type, url, dsSetId, dataSourcetype);
    quboleDao.insert(id, dbTapId, authToken);
    return id;
  }
}
