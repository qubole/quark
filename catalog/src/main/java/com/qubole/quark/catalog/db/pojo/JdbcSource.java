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

package com.qubole.quark.catalog.db.pojo;

import java.util.Map;

/**
 * An extension of {@link DataSource}
 * that is constructed from database.
 */
public class JdbcSource extends DataSource {
  private String username;
  private String password;

  public JdbcSource(long id,
                    String type,
                    String name,
                    String datasourceType,
                    String url,
                    long dsSetId,
                    String username,
                    String password) {
    super(id, type, name, datasourceType, url, dsSetId);
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public Map<String, Object> getProperties(Map<String, Object> prop) {
    prop.put("username", this.username);
    prop.put("password", this.password);
    prop.put("factory", "com.qubole.quark.plugins.jdbc.JdbcFactory");

    return prop;
  }

  @Override
  public String[] values() {
    return new String[]{String.valueOf(this.getId()),
        this.getType(),
        this.getUrl(),
        this.getName(),
        String.valueOf(this.getDsSetId()),
        this.getDatasourceType(),
        null,
        null,
        this.username,
        this.password};
  }
}
