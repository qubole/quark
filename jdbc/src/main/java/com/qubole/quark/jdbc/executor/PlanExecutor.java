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
package com.qubole.quark.jdbc.executor;

import com.qubole.quark.jdbc.QuarkMetaResultSet;
import com.qubole.quark.planner.parser.ParserResult;

/**
 * Created by amoghm on 3/4/16.
 */
public interface PlanExecutor {
  QuarkMetaResultSet execute(ParserResult result) throws Exception;
}
