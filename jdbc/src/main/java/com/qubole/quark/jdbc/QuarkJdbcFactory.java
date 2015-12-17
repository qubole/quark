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

package com.qubole.quark.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.jdbc.CalciteRootSchema;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Partial implementation of {@link org.apache.calcite.avatica.AvaticaFactory}
 * (factory for main JDBC objects) for Drill's JDBC driver.
 * <p>
 * Handles JDBC version number.
 * </p>
 */
public abstract class QuarkJdbcFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  /**
   * Creates a JDBC factory with given major/minor version number.
   */
  protected QuarkJdbcFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public int getJdbcMajorVersion() {
    return major;
  }

  @Override
  public int getJdbcMinorVersion() {
    return minor;
  }


  /**
   * Creates a Quark connection for Avatica (in terms of Avatica types).
   * <p>
   * This implementation delegates to
   * {@link #newConnection(QuarkDriver, QuarkJdbcFactory, String, Properties)}.
   * </p>
   */
  @Override
  public final AvaticaConnection newConnection(UnregisteredDriver driver,
                                               AvaticaFactory factory,
                                               String url,
                                               Properties info) throws SQLException {
    return newConnection((QuarkDriver) driver, (QuarkJdbcFactory) factory, url, info,
        null, null);
  }

  /**
   * Creates a Quark connection (in terms of Quark-specific types).
   */
  protected abstract QuarkConnectionImpl newConnection(QuarkDriver driver,
                                                       QuarkJdbcFactory factory,
                                                       String url,
                                                       Properties info,
                                                       CalciteRootSchema rootSchema,
                                                       JavaTypeFactory typeFactory)
      throws SQLException;
}
