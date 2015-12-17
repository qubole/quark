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

import java.util.Properties;

/**
 * Factory for {@link QuarkSchema} objects.
 */
public interface QuarkFactory {
  /**
   * Creates a Schema.
   *
   * @param parentSchema Parent schema
   * @param name         Name of this schema
   * @param operand      The "operand" JSON property
   * @return Created schema
   */
  QuarkFactoryResult create(Properties info) throws QuarkException;
}
