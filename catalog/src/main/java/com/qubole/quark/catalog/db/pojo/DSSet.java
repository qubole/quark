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

/**
 * POJO for DSSet in database to logically group
 * together data-sources, cubes and views.
 */
public class DSSet {
  private long id;
  private long defaultDatasourceId;
  private String name;

  public DSSet(long id, String name, long defaultDatasourceId) {
    this.id = id;
    this.name = name;
    this.defaultDatasourceId = defaultDatasourceId;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getDefaultDatasourceId() {
    return defaultDatasourceId;
  }

  public void setDefaultDataasourceId(long defaultDatasourceId) {
    this.defaultDatasourceId = defaultDatasourceId;
  }
}
