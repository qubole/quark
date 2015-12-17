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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Type of a field among all data storages.
 */
public enum FieldType {
  STRING_1(String.class, "string"),
  STRING_2(String.class, "character varying"),
  STRING_3(String.class, "character"),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT_1(Primitive.INT),
  INT_2(Integer.class, "integer"),
  INT_3(Short.class, "smallint"),
  INT_4(long.class, "bigint"),
  INT_5(Byte.class, "tinyint"),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(java.sql.Date.class, "date"),
  TIME(java.sql.Time.class, "time"),
  TIMESTAMP(java.sql.Timestamp.class, "timestamp");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, FieldType> MAP =
      new HashMap<String, FieldType>();

  static {
    for (FieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  FieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  FieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    return typeFactory.createJavaType(clazz);
  }

  public static FieldType of(String typeString) {
    return MAP.get(typeString);
  }
}
