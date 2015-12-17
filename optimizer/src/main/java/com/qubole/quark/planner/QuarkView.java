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

package com.qubole.quark.planner;

import java.util.List;

/**
 * Contains View Desciption from which we can generate {@link QuarkViewTable}
 */
public class QuarkView {
  public final String name;
  public final String viewSql;
  public final String table;
  public final List<String> schema;
  public final List<String> alias;

  public QuarkView(String name, String viewSql, String table, List<String> schema,
                   List<String> alias) {
    this.name = name;
    this.viewSql = viewSql;
    this.table = table;
    this.schema = schema;
    this.alias = alias;
  }
}
