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

import com.qubole.quark.catalog.db.dao.QuboleDbSourceDAO;
import com.qubole.quark.catalog.db.encryption.AESEncrypt;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Created by adeshr on 2/17/16.
 */
public class QuboleDbSourceTest extends DbUtility {

  private static final String dbSchemaUrl = "jdbc:h2:mem:QuboleDbSourceTest;DB_CLOSE_DELAY=-1";
  private static QuboleDbSourceDAO quboleDbSourceDAO;
  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException,
      IOException, URISyntaxException {

    setUpDb(dbSchemaUrl, "sa", "", "tpcds.sql");
    setUpDb(dbSchemaUrl, "sa", "", "tpcds_2.sql");

    DBI dbi = new DBI(dbSchemaUrl, "sa", "");
    quboleDbSourceDAO = dbi.onDemand(QuboleDbSourceDAO.class);
    dbi.define("encryptClass", new AESEncrypt("easy"));
  }

  @Test
  public void testGet() {
    List<QuboleDbSource> quboleDbSources = quboleDbSourceDAO.findByDSSetId(1);
    assertThat(quboleDbSources.size(), equalTo(1));
  }

  @Test
  public void testGetSuccessOn1() {
    QuboleDbSource quboleDbSource = quboleDbSourceDAO.find(4, 1);
    assertThat(quboleDbSource.getName(), equalTo("TEST"));
    assertThat(quboleDbSource.getDsSetId(), equalTo(1L));
  }

  @Test
  public void testGetSuccessOn2() {
    QuboleDbSource quboleDbSource = quboleDbSourceDAO.find(8, 10);
    assertThat(quboleDbSource.getName(), equalTo("TEST_10"));
    assertThat(quboleDbSource.getDsSetId(), equalTo(10L));
  }

  @Test
  public void testGetFail() {
    QuboleDbSource quboleDbSource = quboleDbSourceDAO.find(8, 1);
    assertThat(quboleDbSource, nullValue());
  }

}
