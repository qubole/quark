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

import com.qubole.quark.QuarkException;

import java.util.List;
import java.util.Properties;

/**
 * Created by rajatv on 11/12/15.
 */
public abstract class TestFactory {
  private final QuarkSchema defaultSchema;

  protected TestFactory(QuarkSchema defaultSchema) {
    this.defaultSchema = defaultSchema;
  }
  public abstract List<QuarkSchema> create(Properties info) throws QuarkException;

  public QuarkSchema getDefaultSchema() {
    return defaultSchema;
  }
}
