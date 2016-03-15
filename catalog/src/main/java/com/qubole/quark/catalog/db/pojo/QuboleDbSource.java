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
public class QuboleDbSource extends DataSource {
  private long dbTapId;
  private String authToken;

  public QuboleDbSource(long id,
                    String type,
                    String name,
                    String datasourceType,
                    String url,
                    long dsSetId,
                    long dbTapId,
                    String authToken) {
    super(id, type, name, datasourceType, url, dsSetId);
    this.dbTapId = dbTapId;
    this.authToken = authToken;
  }

  public Map<String, Object> getProperties(Map<String, Object> properties) {
    properties.put("token", this.getAuthToken());
    if (this.getDbTapId() != 0) {
      properties.put("dbtapid", this.getDbTapId());
    }
    properties.put("endpoint", super.getUrl());
    properties.put("factory", "com.qubole.quark.plugins.qubole.QuboleFactory");
    return properties;
  }

  public long getDbTapId() {
    return dbTapId;
  }

  public String getAuthToken() {
    return authToken;
  }

  public void setDbTapId(long dbTapId) {
    this.dbTapId = dbTapId;
  }

  public void setAuthToken(String authToken) {
    this.authToken = authToken;
  }
}
