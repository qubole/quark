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

import com.qubole.quark.fatjdbc.test.utility.MetaDataTest;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by adeshr on 2/18/16.
 */
public class JsonMetaDataTest extends MetaDataTest {
  private static final Logger log = LoggerFactory.getLogger(JsonMetaDataTest.class);

  static {
    h2Url = "jdbc:h2:mem:MetaDataTest1;DB_CLOSE_DELAY=-1";
    props = new Properties();
    String jsonTestString =
        "{" +
            "\"version\":\"2.0\"," +
            "\"dataSources\":" +
            " [" +
            "   {" +
            "     \"type\":\"H2\"," +
            "     \"url\":\"" + h2Url + "\"," +
            "     \"factory\":\"com.qubole.quark.plugins.jdbc.JdbcFactory\"," +
            "     \"username\":\"sa\"," +
            "     \"password\":\"\"," +
            "     \"default\":\"true\"," +
            "     \"name\":\"H2\"" +
            "   }" +
            " ]," +
            " \"defaultDataSource\":0" +
            "}";
    props.put("model", jsonTestString);
  }

  @BeforeClass
  public static void setUpClass()  throws Exception {
    MetaDataTest.setUpClass(h2Url);
  }

  protected String getConnectionUrl() {
    return "jdbc:quark:fat:json:";
  }

}
