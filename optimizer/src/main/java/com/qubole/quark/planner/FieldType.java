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

import java.sql.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Type of a field among all data storages.
 */
public enum FieldType {
  // Refer to table 8.9.1 in
  // https://docs.oracle.com/javase/6/docs/technotes/guides/jdbc/getstart/mapping.html
  CHAR(Types.CHAR, String.class),
  VARCHAR(Types.VARCHAR, String.class),
  LONGVARCHAR(Types.LONGVARCHAR, String.class),
  NUMERIC(Types.NUMERIC, java.math.BigDecimal.class),
  DECIMAL(Types.DECIMAL, Primitive.DOUBLE), //java.math.BigDecimal.class),
  BIT(Types.BIT, Primitive.BOOLEAN),
  TINYINT(Types.TINYINT, Primitive.BYTE),
  SHORT(Types.SMALLINT, Primitive.SHORT),
  INT(Types.INTEGER, Primitive.INT),
  BIGINT(Types.BIGINT, Primitive.LONG),
  REAL(Types.REAL, Primitive.FLOAT),
  FLOAT(Types.FLOAT, Primitive.FLOAT),
  DOUBLE(Types.DOUBLE, Primitive.DOUBLE),
  BINARY(Types.BINARY, byte[].class),
  VARBINARY(Types.VARBINARY, byte[].class),
  LONGVARBINARY(Types.LONGVARBINARY, byte[].class),
  DATE(Types.DATE, java.sql.Date.class),
  TIME(Types.TIME, java.sql.Time.class),
  TIMESTAMP(Types.TIMESTAMP, java.sql.Timestamp.class);

  private final Class clazz;
  private final int type;

  private static final Map<Integer, FieldType> MAP =
      new HashMap<>();

  static {
    for (FieldType value : values()) {
      MAP.put(value.type, value);
    }
  }

  FieldType(int type, Primitive primitive) {
    this(type, primitive.boxClass);
  }

  FieldType(int type, Class clazz) {
    this.clazz = clazz;
    this.type = type;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    return typeFactory.createJavaType(clazz);
  }

  public static FieldType of(Integer i) {
    return MAP.get(i);
  }
}
