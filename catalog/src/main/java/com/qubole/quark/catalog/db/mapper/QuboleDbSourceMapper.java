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

package com.qubole.quark.catalog.db.mapper;

import com.qubole.quark.catalog.db.encryption.MysqlAES;
import com.qubole.quark.catalog.db.pojo.QuboleDbSource;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Mapper for {@link QuboleDbSource}
 */
public class QuboleDbSourceMapper implements ResultSetMapper<QuboleDbSource> {
  public QuboleDbSource map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    MysqlAES mysqlAES = MysqlAES.getInstance();

    return new QuboleDbSource(r.getInt("id"), r.getString("type"), r.getString("name"),
        r.getString("datasource_type"), mysqlAES.convertToEntityAttribute(r.getString("url")),
        r.getInt("ds_set_id"), r.getLong("dbtap_id"),
        mysqlAES.convertToEntityAttribute(r.getString("auth_token")));
  }
}
