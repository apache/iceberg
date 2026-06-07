/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.connector.catalog.ColumnDefaultValue;
import org.apache.spark.sql.connector.catalog.DefaultValue;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

class SparkDefaultValues {

  private SparkDefaultValues() {}

  static Literal<?> toIcebergLiteral(String columnName, Type type, DefaultValue defaultValue) {
    if (defaultValue == null) {
      return null;
    }

    Expression expression = defaultValue.getExpression();
    if (expression instanceof org.apache.spark.sql.connector.expressions.Literal) {
      org.apache.spark.sql.connector.expressions.Literal<?> literal =
          (org.apache.spark.sql.connector.expressions.Literal<?>) expression;
      return toIcebergLiteral(columnName, type, literal.value(), defaultValue.getSql());
    } else if (defaultValue instanceof ColumnDefaultValue) {
      ColumnDefaultValue columnDefault = (ColumnDefaultValue) defaultValue;
      org.apache.spark.sql.connector.expressions.Literal<?> literal = columnDefault.getValue();
      if (literal != null) {
        return toIcebergLiteral(columnName, type, literal.value(), defaultValue.getSql());
      }
    }

    throw new UnsupportedOperationException(
        String.format(
            "Column default for %s must be a literal value: %s",
            columnName, defaultValue.getSql()));
  }

  private static Literal<?> toIcebergLiteral(
      String columnName, Type type, Object sparkValue, String defaultSql) {
    if (sparkValue == null) {
      return null;
    }

    Preconditions.checkArgument(
        type.isPrimitiveType() && type.typeId() != Type.TypeID.UNKNOWN,
        "Column default for %s with type %s is not supported by Iceberg Spark DDL: %s",
        columnName,
        type,
        defaultSql);

    Literal<?> literal = createLiteral(type, sparkValue).to(type);
    Preconditions.checkArgument(
        literal != null,
        "Column default for %s cannot be converted to Iceberg type %s: %s",
        columnName,
        type,
        defaultSql);
    return literal;
  }

  private static Literal<?> createLiteral(Type type, Object sparkValue) {
    if (sparkValue instanceof UTF8String) {
      return Literal.of(sparkValue.toString());
    }
    if (sparkValue instanceof Decimal) {
      return Literal.of(((Decimal) sparkValue).toJavaBigDecimal());
    }
    if (sparkValue instanceof byte[]) {
      return Literal.of(ByteBuffer.wrap((byte[]) sparkValue));
    }

    switch (type.typeId()) {
      case DATE:
        if (sparkValue instanceof Number) {
          return Literal.of(((Number) sparkValue).intValue());
        }
        break;
      case TIME:
      case TIMESTAMP:
        if (sparkValue instanceof Number) {
          return Literal.of(((Number) sparkValue).longValue());
        }
        break;
      default:
    }

    Object value = SparkValueConverter.convert(type, sparkValue);

    switch (type.typeId()) {
      case BOOLEAN:
        return Literal.of((Boolean) value);
      case INTEGER:
      case DATE:
        return Literal.of(((Number) value).intValue());
      case LONG:
      case TIME:
      case TIMESTAMP:
        return Literal.of(((Number) value).longValue());
      case FLOAT:
        return Literal.of(((Number) value).floatValue());
      case DOUBLE:
        return Literal.of(((Number) value).doubleValue());
      case DECIMAL:
        return Literal.of((BigDecimal) value);
      case STRING:
        return Literal.of((CharSequence) value);
      case BINARY:
      case FIXED:
        if (value instanceof byte[]) {
          return Literal.of(ByteBuffer.wrap((byte[]) value));
        }
        return Literal.of((ByteBuffer) value);
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for column default literal: " + type);
    }
  }
}
