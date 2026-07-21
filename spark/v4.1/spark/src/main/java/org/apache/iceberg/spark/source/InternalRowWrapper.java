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
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Class to adapt a Spark {@code InternalRow} to Iceberg {@link StructLike} for uses like {@link
 * org.apache.iceberg.PartitionKey#partition(StructLike)}
 */
class InternalRowWrapper implements StructLike {
  private final DataType[] types;
  private final BiFunction<InternalRow, Integer, ?>[] getters;
  private InternalRow row = null;

  @SuppressWarnings("unchecked")
  InternalRowWrapper(StructType rowType, Types.StructType icebergSchema) {
    this.types = Stream.of(rowType.fields()).map(StructField::dataType).toArray(DataType[]::new);
    Preconditions.checkArgument(
        types.length == icebergSchema.fields().size(),
        "Invalid length: Spark struct type (%s) != Iceberg struct type (%s)",
        types.length,
        icebergSchema.fields().size());
    this.getters = new BiFunction[types.length];
    for (int i = 0; i < types.length; i++) {
      getters[i] = getter(icebergSchema.fields().get(i).type(), types[i]);
    }
  }

  InternalRowWrapper wrap(InternalRow internalRow) {
    this.row = internalRow;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (row.isNullAt(pos)) {
      return null;
    } else if (getters[pos] != null) {
      return javaClass.cast(getters[pos].apply(row, pos));
    }

    return javaClass.cast(row.get(pos, types[pos]));
  }

  @Override
  public <T> void set(int pos, T value) {
    row.update(pos, value);
  }

  private static BiFunction<InternalRow, Integer, ?> getter(Type icebergType, DataType type) {
    if (type instanceof StringType) {
      // Spark represents UUIDs as strings
      if (Type.TypeID.UUID == icebergType.typeId()) {
        return (row, pos) -> UUID.fromString(row.getUTF8String(pos).toString());
      }

      return (row, pos) -> row.getUTF8String(pos).toString();
    } else if (type instanceof DecimalType) {
      DecimalType decimal = (DecimalType) type;
      return (row, pos) ->
          row.getDecimal(pos, decimal.precision(), decimal.scale()).toJavaBigDecimal();
    } else if (type instanceof BinaryType) {
      return (row, pos) -> ByteBuffer.wrap(row.getBinary(pos));
    } else if (type instanceof StructType) {
      StructType structType = (StructType) type;
      InternalRowWrapper nestedWrapper =
          new InternalRowWrapper(structType, icebergType.asStructType());
      return (row, pos) -> nestedWrapper.wrap(row.getStruct(pos, structType.size()));
    }

    return null;
  }
}
