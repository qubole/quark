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

import com.qubole.quark.QuarkException;
import com.qubole.quark.catalog.db.dao.DSSetDAO;
import com.qubole.quark.catalog.db.encryption.AESEncrypt;
import com.qubole.quark.catalog.db.encryption.NoopEncrypt;
import com.qubole.quark.catalog.db.pojo.DSSet;

import org.flywaydb.core.Flyway;

import org.skife.jdbi.v2.DBI;

import java.util.List;
import java.util.Properties;

/**
 * Created by rajatv on 6/1/16.
 */
public class Connection {
  private final DBI dbi;
  private final Properties info;
  private DSSet dsSet;

  public Connection(Properties info) {
    this.info = info;
    this.dbi = new DBI(
        info.getProperty("url"),
        info.getProperty("user"),
        info.getProperty("password"));

    if (Boolean.parseBoolean(info.getProperty("encrypt", "false"))) {
      dbi.define("encryptClass", new AESEncrypt(info.getProperty("encryptionKey")));
    } else {
      dbi.define("encryptClass", new NoopEncrypt());
    }
  }

  public void runFlyWay() {
    Flyway flyway = new Flyway();
    flyway.setDataSource(
        info.getProperty("url"),
        info.getProperty("user"),
        info.getProperty("password"));
    flyway.migrate();
  }

  public DSSet getDSSet() {
    if (this.dsSet == null) {
      DSSetDAO dsSetDAO = dbi.onDemand(DSSetDAO.class);
      if (info.containsKey("dsSetId")) {
        this.dsSet = dsSetDAO.find(Integer.parseInt(info.getProperty("dsSetId")));
      } else {
        List<DSSet> dsSets = dsSetDAO.findAll();
        this.dsSet = dsSets.get(0);
      }
      dbi.define("dsSetId", dsSet.getId());
    }
    return dsSet;
  }

  public DBI getDbi() throws QuarkException {
    if (dsSet == null) {
      throw new QuarkException("DB Catalog not initialized correctly. DSSet is not found");
    }
    return this.dbi;
  }
}
