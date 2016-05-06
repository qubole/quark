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
import com.qubole.quark.catalog.db.mapper.JdbcSourceMapper;
import com.qubole.quark.catalog.db.pojo.JdbcSource;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.Define;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;

import java.sql.SQLException;
import java.util.List;

/**
 * DAO for {@link JdbcSource}
 */
@UseStringTemplate3StatementLocator
@RegisterMapper(JdbcSourceMapper.class)
public abstract class JdbcSourceDAO {
  @SqlQuery("select ds.id, ds.name, ds.type, ds.datasource_type, ds.url, ds.ds_set_id, "
      + "js.username, js.password from data_sources ds join jdbc_sources js on ds.id = js.id "
      + "where ds.ds_set_id = :ds_set_id")
  public abstract List<JdbcSource> findByDSSetId(@Bind("ds_set_id") long dsSetId);

  @SqlQuery("select ds.id, ds.name, ds.type, ds.datasource_type, ds.url, ds.ds_set_id, "
      + "js.username, js.password from data_sources ds join jdbc_sources js on ds.id = js.id "
      + "where ds.id = :id")
  public abstract JdbcSource find(@Bind("id") int id);

  @SqlUpdate("insert into jdbc_sources(id, username, password) values(:id, :username, :password)")
  abstract void insert(@Bind("id") long id, @Bind("username") String username,
      @Bind("password") String password);

  @GetGeneratedKeys
  @SqlUpdate("update jdbc_sources set username = :j.username,"
      + " password = :j.password where id = :j.id")
  protected abstract int updateJdbc(@BindBean("j") JdbcSource source);

  @SqlUpdate("delete from jdbc_sources where id = :id")
  public abstract void delete(@Bind("id") int id);

  @Transaction
  public int update(JdbcSource source, DataSourceDAO dao, Encrypt encrypt) {
    try {
      source.setUrl(encrypt.convertToDatabaseColumn(source.getUrl()));
      source.setUsername(encrypt.convertToDatabaseColumn(source.getUsername()));
      source.setPassword(encrypt.convertToDatabaseColumn(source.getPassword()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    updateJdbc(source);
    return dao.update(source);
  }

  @SqlQuery("select data_sources.id, name, type, datasource_type, url, ds_set_id, "
      + "jdbc_sources.username, jdbc_sources.password from data_sources join jdbc_sources "
      + "on data_sources.id = jdbc_sources.id <where>")
  public abstract List<JdbcSource> findByWhere(@Define("where") String where);
}
