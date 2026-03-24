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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Converts Arrow VectorSchemaRoot batches from Lance into Spark InternalRow objects.
 *
 * <p>Iterates rows within each batch, extracting values from Arrow vectors and converting them to
 * Spark internal types.
 */
public class SparkLanceRowReader {

  private SparkLanceRowReader() {}

  /**
   * Builds a reader function that converts Arrow batches to InternalRow.
   *
   * @param expectedSchema the Iceberg schema defining expected columns
   * @param idToConstant constant values for partition/metadata columns
   * @return a function that converts (VectorSchemaRoot, idToConstant) → CloseableIterable
   */
  public static Function<
          Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<InternalRow>>
      buildReader(Schema expectedSchema, Map<Integer, ?> idToConstant) {
    return entry -> {
      VectorSchemaRoot batch = entry.getKey();
      Map<Integer, ?> constants = entry.getValue();
      int rowCount = batch.getRowCount();

      List<InternalRow> rows =
          org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayListWithCapacity(
              rowCount);
      List<Types.NestedField> columns = expectedSchema.columns();
      int numCols = columns.size();

      for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        Object[] values = new Object[numCols];
        for (int colIdx = 0; colIdx < numCols; colIdx++) {
          Types.NestedField field = columns.get(colIdx);

          if (constants != null && constants.containsKey(field.fieldId())) {
            values[colIdx] = convertToSparkConstant(constants.get(field.fieldId()), field);
          } else {
            FieldVector vector = batch.getVector(field.name());
            if (vector == null || vector.isNull(rowIdx)) {
              values[colIdx] = null;
            } else {
              values[colIdx] = readSparkValue(vector, rowIdx, field);
            }
          }
        }
        rows.add(new GenericInternalRow(values));
      }

      return new CloseableIterable<InternalRow>() {
        @Override
        public CloseableIterator<InternalRow> iterator() {
          return CloseableIterator.withClose(rows.iterator());
        }

        @Override
        public void close() throws IOException {}
      };
    };
  }

  private static Object readSparkValue(FieldVector vector, int rowIdx, Types.NestedField field) {
    switch (field.type().typeId()) {
      case BOOLEAN:
        return ((BitVector) vector).get(rowIdx) == 1;
      case INTEGER:
        return ((IntVector) vector).get(rowIdx);
      case LONG:
        return ((BigIntVector) vector).get(rowIdx);
      case FLOAT:
        return ((Float4Vector) vector).get(rowIdx);
      case DOUBLE:
        return ((Float8Vector) vector).get(rowIdx);
      case DATE:
        return ((DateDayVector) vector).get(rowIdx);
      case TIME:
        return ((TimeMicroVector) vector).get(rowIdx);
      case TIMESTAMP:
        if (((Types.TimestampType) field.type()).shouldAdjustToUTC()) {
          return ((TimeStampMicroTZVector) vector).get(rowIdx);
        } else {
          return ((TimeStampMicroVector) vector).get(rowIdx);
        }
      case STRING:
        byte[] strBytes = ((VarCharVector) vector).get(rowIdx);
        return UTF8String.fromBytes(strBytes);
      case UUID:
        byte[] uuidBytes = ((FixedSizeBinaryVector) vector).get(rowIdx);
        return uuidBytes;
      case FIXED:
        return ((FixedSizeBinaryVector) vector).get(rowIdx);
      case BINARY:
        return ((VarBinaryVector) vector).get(rowIdx);
      case DECIMAL:
        BigDecimal bd = ((DecimalVector) vector).getObject(rowIdx);
        Types.DecimalType decType = (Types.DecimalType) field.type();
        return Decimal.apply(bd, decType.precision(), decType.scale());
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for Spark row reader: " + field.type().typeId());
    }
  }

  private static Object convertToSparkConstant(Object value, Types.NestedField field) {
    if (value == null) {
      return null;
    }
    switch (field.type().typeId()) {
      case STRING:
        return UTF8String.fromString(value.toString());
      case DECIMAL:
        Types.DecimalType decType = (Types.DecimalType) field.type();
        return Decimal.apply((BigDecimal) value, decType.precision(), decType.scale());
      default:
        return value;
    }
  }
}
