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

package com.qubole.quark.plugins.qubole;

import com.qubole.qds.sdk.java.entities.NameTypePosition;

import java.util.Comparator;

/**
 * Created by dev on 11/13/15.
 */
class ColumnComparator implements Comparator<NameTypePosition> {
  @Override
  public int compare(NameTypePosition col1, NameTypePosition col2) {
    return Integer.parseInt(col1.getOrdinal_position())
            - (Integer.parseInt(col2.getOrdinal_position()));
  }
}
