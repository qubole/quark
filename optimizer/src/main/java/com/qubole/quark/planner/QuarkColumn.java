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

/**
 * Represents a column in a table.
 */
public class QuarkColumn {
  public final String name;
  public final int  type;

  public QuarkColumn(String name, int type) {
    this.name = name.toUpperCase();
    this.type = type;
  }

  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (this.getClass() != obj.getClass()) return false;
    QuarkColumn other = (QuarkColumn) obj;
    return (this.name.equals(other.name) && this.type == other.type);
  }

  public int hashCode() {
    return name.hashCode() + type * 31;
  }
}
