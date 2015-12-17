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

import org.apache.calcite.linq4j.Enumerator;

/**
 * Created by rajatv on 2/16/15.
 */
class QuarkEnumerator implements Enumerator<Object> {
  /**
   * Returns an array of integers {0, ..., n - 1}.
   */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  QuarkEnumerator() {
  }

  public Object current() {
    return null;
  }

  public boolean moveNext() {
    return false;
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
  }
}
