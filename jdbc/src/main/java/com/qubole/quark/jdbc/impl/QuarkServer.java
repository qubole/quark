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

import com.google.common.collect.Maps;

import com.qubole.quark.jdbc.QuarkConnection;
import com.qubole.quark.jdbc.QuarkConnectionImpl;
import com.qubole.quark.jdbc.QuarkJdbcStatement;

import java.util.Map;

/**
 * Implementation of Server.
 */
public class QuarkServer {
  final Map<Integer, QuarkJdbcStatement> statementMap = Maps.newHashMap();

  public void removeStatement(Meta.StatementHandle h) {
    statementMap.remove(h.id);
  }

  public void addStatement(QuarkConnection connection,
                           Meta.StatementHandle h) {
    final QuarkConnectionImpl c = (QuarkConnectionImpl) connection;
    statementMap.put(h.id, new QuarkJdbcStatementImpl(c));
  }

  public QuarkJdbcStatement getStatement(Meta.StatementHandle h) {
    return statementMap.get(h.id);
  }
}
