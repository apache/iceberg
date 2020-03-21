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

package org.apache.iceberg.spark.source;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Objects of this class generate an {@link InternalRow} by utilizing the partition schema passed during construction.
 */
class PartitionRowConverter implements Function<StructLike, InternalRow> {
  private final DataType[] types;
  private final int[] positions;
  private final Class<?>[] javaTypes;
  private final GenericInternalRow reusedRow;

  PartitionRowConverter(Schema partitionSchema, PartitionSpec spec) {
    StructType partitionType = SparkSchemaUtil.convert(partitionSchema);
    StructField[] fields = partitionType.fields();

    this.types = new DataType[fields.length];
    this.positions = new int[types.length];
    this.javaTypes = new Class<?>[types.length];
    this.reusedRow = new GenericInternalRow(types.length);

    List<PartitionField> partitionFields = spec.fields();
    for (int rowIndex = 0; rowIndex < fields.length; rowIndex += 1) {
      this.types[rowIndex] = fields[rowIndex].dataType();

      int sourceId = partitionSchema.columns().get(rowIndex).fieldId();
      for (int specIndex = 0; specIndex < partitionFields.size(); specIndex += 1) {
        PartitionField field = spec.fields().get(specIndex);
        if (field.sourceId() == sourceId && "identity".equals(field.transform().toString())) {
          positions[rowIndex] = specIndex;
          javaTypes[rowIndex] = spec.javaClasses()[specIndex];
          break;
        }
      }
    }
  }

  @Override
  public InternalRow apply(StructLike tuple) {
    for (int i = 0; i < types.length; i += 1) {
      Object value = tuple.get(positions[i], javaTypes[i]);
      if (value != null) {
        reusedRow.update(i, convert(value, types[i]));
      } else {
        reusedRow.setNullAt(i);
      }
    }

    return reusedRow;
  }

  /**
   * Converts the objects into instances used by Spark's InternalRow.
   *
   * @param value a data value
   * @param type  the Spark data type
   * @return the value converted to the representation expected by Spark's InternalRow.
   */
  private static Object convert(Object value, DataType type) {
    if (type instanceof StringType) {
      return UTF8String.fromString(value.toString());
    } else if (type instanceof BinaryType) {
      return ByteBuffers.toByteArray((ByteBuffer) value);
    } else if (type instanceof DecimalType) {
      return Decimal.fromDecimal(value);
    }
    return value;
  }
}
