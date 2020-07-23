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

package org.apache.iceberg.flink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

/**
 * Converter between Flink types and Iceberg type.
 * The conversion is not a 1:1 mapping that not allows back-and-forth conversion. So some information might get lost
 * during the back-and-forth conversion.
 * <p>
 * This inconsistent types:
 * <ul>
 *   <li>map Iceberg UUID type to Flink BinaryType(16)</li>
 *   <li>map Flink VarCharType(_) and CharType(_) to Iceberg String type</li>
 *   <li>map Flink VarBinaryType(_) to Iceberg Binary type</li>
 *   <li>map Flink TimeType(_) to Iceberg Time type (microseconds)</li>
 *   <li>map Flink TimestampType(_) to Iceberg Timestamp without zone type (microseconds)</li>
 *   <li>map Flink LocalZonedTimestampType(_) to Iceberg Timestamp with zone type (microseconds)</li>
 *   <li>map Flink MultiSetType to Iceberg Map type(element, int)</li>
 * </ul>
 * <p>
 */
public class FlinkSchemaUtil {

  private FlinkSchemaUtil() {
  }

  /**
   * Convert the flink table schema to apache iceberg schema.
   */
  public static Schema convert(TableSchema schema) {
    LogicalType schemaType = schema.toRowDataType().getLogicalType();
    Preconditions.checkArgument(schemaType instanceof RowType, "Schema logical type should be RowType.");

    RowType root = (RowType) schemaType;
    Type converted = root.accept(new FlinkTypeToType(root));

    return new Schema(converted.asStructType().fields());
  }

  /**
   * Convert a {@link Schema} to a {@link RowType Flink type}.
   *
   * @param schema a Schema
   * @return the equivalent Flink type
   * @throws IllegalArgumentException if the type cannot be converted to Flink
   */
  public static RowType convert(Schema schema) {
    return (RowType) TypeUtil.visit(schema, new TypeToFlinkType());
  }

  /**
   * Convert a {@link Type} to a {@link LogicalType Flink type}.
   *
   * @param type a Type
   * @return the equivalent Flink type
   * @throws IllegalArgumentException if the type cannot be converted to Flink
   */
  public static LogicalType convert(Type type) {
    return TypeUtil.visit(type, new TypeToFlinkType());
  }

  /**
   * Convert a {@link RowType} to a {@link TableSchema}.
   *
   * @param rowType a RowType
   * @return Flink TableSchema
   */
  public static TableSchema toSchema(RowType rowType) {
    TableSchema.Builder builder = TableSchema.builder();
    for (RowType.RowField field : rowType.getFields()) {
      builder.field(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }
    return builder.build();
  }
}
