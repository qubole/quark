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

package com.qubole.quark.jdbc.impl;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.server.CalciteServerStatement;

import com.google.common.base.Preconditions;

import com.qubole.quark.jdbc.QuarkConnection;
import com.qubole.quark.jdbc.QuarkConnectionImpl;
import com.qubole.quark.jdbc.QuarkJdbcStatement;

import java.util.Iterator;

/**
 * Implementation of {@link CalciteServerStatement}.
 */
public class QuarkJdbcStatementImpl
    implements QuarkJdbcStatement {
  private final QuarkConnectionImpl connection;
  private Iterator<Object> iterator;
  private Meta.Signature signature;

  public QuarkJdbcStatementImpl(QuarkConnectionImpl connection) {
    this.connection = Preconditions.checkNotNull(connection);
  }

  public QuarkConnection getConnection() {
    return connection;
  }

  public void setSignature(Meta.Signature signature) {
    this.signature = signature;
  }

  public Meta.Signature getSignature() {
    return signature;
  }

  public Iterator<Object> getResultSet() {
    return iterator;
  }

  public void setResultSet(Iterator<Object> iterator) {
    this.iterator = iterator;
  }
}
