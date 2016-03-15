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

package com.qubole.quark.fatjdbc.test;

import com.qubole.quark.fatjdbc.test.utility.SelectTest;
import org.junit.BeforeClass;

import java.util.Properties;

/**
 * Created by priyankak on 3/3/16.
 */
public class GenericDbSelectTest extends SelectTest{

    static {
        dbUrl = "jdbc:h2:mem:GenericDbTest1;DB_CLOSE_DELAY=-1";
        props = new Properties();
        String jsonTestString =
                "{" +
                        "\"version\":\"2.0\"," +
                        "\"dataSources\":" +
                        " [" +
                        "   {" +
                        "     \"type\":\"GENERIC\"," +
                        "     \"url\":\"" + dbUrl + "\"," +
                        "     \"factory\":\"com.qubole.quark.plugins.jdbc.JdbcFactory\"," +
                        "     \"username\":\"sa\"," +
                        "     \"password\":\"\"," +
                        "     \"default\":\"true\"," +
                        "     \"defaultSchema\": \"PUBLIC\"," +
                        "     \"catalogSql\": \"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION\"," +
                        "     \"productName\": \"H2\"," +
                        "     \"isCaseSensitive\": \"true\"," +
                        "     \"name\":\"H2\"" +
                        "   }" +
                        " ]," +
                        " \"defaultDataSource\":0" +
                        "}";
        props.put("model", jsonTestString);
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        SelectTest.setUpClass(dbUrl);
    }

  protected String getConnectionUrl() {
    return "jdbc:quark:fat:json:";
  }
}
