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

import org.apache.calcite.avatica.Meta;

import java.util.Iterator;

/**
 * Statement within a Quark JDBC Driver.
 */
public interface QuarkJdbcStatement {
  /**
   * Returns the connection.
   */
  QuarkConnection getConnection();

  void setSignature(Meta.Signature signature);

  Meta.Signature getSignature();

  Iterator<Object> getResultSet();

  void setResultSet(Iterator<Object> resultSet);
}
