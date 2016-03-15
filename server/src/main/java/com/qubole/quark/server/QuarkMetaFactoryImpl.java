/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qubole.quark.server;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Bridge between Quark and Avatica.
 */
public class QuarkMetaFactoryImpl implements Meta.Factory {
  protected static final Log LOG = LogFactory.getLog(Main.class);
  public static ServerConfig serverConfig;
  // invoked via reflection
  public QuarkMetaFactoryImpl() {
    super();
  }

  @Override
  public Meta create(List<String> args) {
    String url = "jdbc:quark:fat:db:";

    try {
      if (args.size() == 1) {
        String filePath = args.get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        serverConfig = objectMapper.readValue(new File(filePath), ServerConfig.class);

        url = url + filePath;
      } else {
        throw new RuntimeException(
            "1 argument expected. Received " + Arrays.toString(args.toArray()));
      }
      return new JdbcMeta(url, new Properties());
    } catch (SQLException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
