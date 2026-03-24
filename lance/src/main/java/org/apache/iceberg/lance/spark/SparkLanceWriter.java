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
package org.apache.iceberg.lance.spark;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Converts Spark InternalRow to a Map of column name → value for writing via Lance's Arrow-based
 * writer.
 */
public class SparkLanceWriter {

  private SparkLanceWriter() {}

  /**
   * Builds a writer function that converts InternalRow to a Map.
   *
   * @param icebergSchema the Iceberg schema defining the columns
   * @return a function that extracts values from InternalRow into a name→value Map
   */
  public static Function<InternalRow, Map<String, Object>> buildWriter(Schema icebergSchema) {
    List<Types.NestedField> columns = icebergSchema.columns();
    return row -> {
      Map<String, Object> values = Maps.newHashMapWithExpectedSize(columns.size());
      for (int i = 0; i < columns.size(); i++) {
        Types.NestedField field = columns.get(i);
        if (row.isNullAt(i)) {
          values.put(field.name(), null);
        } else {
          values.put(field.name(), extractValue(row, i, field));
        }
      }
      return values;
    };
  }

  private static Object extractValue(InternalRow row, int ordinal, Types.NestedField field) {
    switch (field.type().typeId()) {
      case BOOLEAN:
        return row.getBoolean(ordinal);
      case INTEGER:
        return row.getInt(ordinal);
      case LONG:
        return row.getLong(ordinal);
      case FLOAT:
        return row.getFloat(ordinal);
      case DOUBLE:
        return row.getDouble(ordinal);
      case DATE:
        // Spark stores dates as int days since epoch
        return row.getInt(ordinal);
      case TIME:
        return row.getLong(ordinal);
      case TIMESTAMP:
        // Spark stores timestamps as long microseconds since epoch
        return row.getLong(ordinal);
      case STRING:
        UTF8String utf8 = row.getUTF8String(ordinal);
        return utf8 != null ? utf8.toString() : null;
      case UUID:
        return row.getBinary(ordinal);
      case FIXED:
        return row.getBinary(ordinal);
      case BINARY:
        return row.getBinary(ordinal);
      case DECIMAL:
        Types.DecimalType decType = (Types.DecimalType) field.type();
        Decimal decimal = row.getDecimal(ordinal, decType.precision(), decType.scale());
        return decimal != null ? decimal.toJavaBigDecimal() : null;
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for Spark writer: " + field.type().typeId());
    }
  }
}
