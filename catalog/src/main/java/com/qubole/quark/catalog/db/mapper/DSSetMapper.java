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

import com.qubole.quark.catalog.db.pojo.DSSet;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Mapper for {@link DSSet}
 */

public class DSSetMapper implements ResultSetMapper<DSSet> {
  public DSSet map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    return new DSSet(r.getInt("id"), r.getString("name"), r.getInt("default_datasource_id"));
  }
}
