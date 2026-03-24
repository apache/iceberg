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
package org.apache.iceberg.lance;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

/**
 * Converts between Iceberg Records and Arrow VectorSchemaRoot batches.
 *
 * <p>Handles the type-level conversion for writing Iceberg records into Arrow vectors and reading
 * Arrow vectors back into Iceberg records.
 */
class LanceArrowConverter {

  private LanceArrowConverter() {}

  /**
   * Write a single row of values into the Arrow VectorSchemaRoot at the given row index.
   *
   * @param batch the target Arrow batch
   * @param rowIndex the row index to write at
   * @param values map of column name to value
   * @param schema the Iceberg schema defining the columns
   */
  static void writeRow(
      VectorSchemaRoot batch, int rowIndex, Map<String, Object> values, Schema schema) {
    for (Types.NestedField field : schema.columns()) {
      Object value = values.get(field.name());
      if (value == null) {
        // Arrow vectors default to null, no action needed
        continue;
      }
      writeValue(batch, field, rowIndex, value);
    }
  }

  /**
   * Read a single row from the Arrow VectorSchemaRoot and return it as an Iceberg Record.
   *
   * @param batch the source Arrow batch
   * @param rowIndex the row index to read from
   * @param schema the Iceberg schema to construct the Record with
   * @param idToConstant constant values to inject for non-data columns (e.g., partition values)
   * @return an Iceberg Record containing the row's data
   */
  static Record readRow(
      VectorSchemaRoot batch, int rowIndex, Schema schema, Map<Integer, ?> idToConstant) {
    GenericRecord record = GenericRecord.create(schema);
    for (Types.NestedField field : schema.columns()) {
      if (idToConstant != null && idToConstant.containsKey(field.fieldId())) {
        record.setField(field.name(), idToConstant.get(field.fieldId()));
        continue;
      }

      if (batch.getVector(field.name()) == null) {
        record.setField(field.name(), null);
        continue;
      }

      Object value = readValue(batch, field.name(), rowIndex, field);
      record.setField(field.name(), value);
    }
    return record;
  }

  /** Convert an Iceberg Record to a Map of column name to value, suitable for writeRow(). */
  static Map<String, Object> recordToMap(Record record, Schema schema) {
    Map<String, Object> map = Maps.newHashMap();
    for (Types.NestedField field : schema.columns()) {
      map.put(field.name(), record.getField(field.name()));
    }
    return map;
  }

  private static void writeValue(
      VectorSchemaRoot batch, Types.NestedField field, int rowIndex, Object value) {
    if (batch.getVector(field.name()) == null) {
      return;
    }

    String vectorName = field.name();
    switch (field.type().typeId()) {
      case BOOLEAN:
        ((BitVector) batch.getVector(vectorName)).setSafe(rowIndex, (Boolean) value ? 1 : 0);
        break;
      case INTEGER:
        ((IntVector) batch.getVector(vectorName)).setSafe(rowIndex, (Integer) value);
        break;
      case LONG:
        ((BigIntVector) batch.getVector(vectorName)).setSafe(rowIndex, (Long) value);
        break;
      case FLOAT:
        ((Float4Vector) batch.getVector(vectorName)).setSafe(rowIndex, (Float) value);
        break;
      case DOUBLE:
        ((Float8Vector) batch.getVector(vectorName)).setSafe(rowIndex, (Double) value);
        break;
      case DECIMAL:
        BigDecimal decimal = (BigDecimal) value;
        ((DecimalVector) batch.getVector(vectorName)).setSafe(rowIndex, decimal);
        break;
      default:
        writeTemporalOrBinaryValue(batch, vectorName, field, rowIndex, value);
        break;
    }
  }

  private static void writeTemporalOrBinaryValue(
      VectorSchemaRoot batch,
      String vectorName,
      Types.NestedField field,
      int rowIndex,
      Object value) {
    switch (field.type().typeId()) {
      case DATE:
        int daysSinceEpoch;
        if (value instanceof LocalDate) {
          daysSinceEpoch = (int) ((LocalDate) value).toEpochDay();
        } else {
          daysSinceEpoch = (Integer) value;
        }
        ((DateDayVector) batch.getVector(vectorName)).setSafe(rowIndex, daysSinceEpoch);
        break;
      case TIME:
        long timeMicros;
        if (value instanceof LocalTime) {
          timeMicros = ((LocalTime) value).getLong(java.time.temporal.ChronoField.MICRO_OF_DAY);
        } else {
          timeMicros = (Long) value;
        }
        ((TimeMicroVector) batch.getVector(vectorName)).setSafe(rowIndex, timeMicros);
        break;
      case TIMESTAMP:
        long tsMicros = toTimestampMicros(value);
        if (((Types.TimestampType) field.type()).shouldAdjustToUTC()) {
          ((TimeStampMicroTZVector) batch.getVector(vectorName)).setSafe(rowIndex, tsMicros);
        } else {
          ((TimeStampMicroVector) batch.getVector(vectorName)).setSafe(rowIndex, tsMicros);
        }
        break;
      default:
        writeStringOrBinaryValue(batch, vectorName, field, rowIndex, value);
        break;
    }
  }

  private static void writeStringOrBinaryValue(
      VectorSchemaRoot batch,
      String vectorName,
      Types.NestedField field,
      int rowIndex,
      Object value) {
    switch (field.type().typeId()) {
      case STRING:
        byte[] strBytes = value.toString().getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) batch.getVector(vectorName))
            .setSafe(rowIndex, strBytes, 0, strBytes.length);
        break;
      case UUID:
        UUID uuid = (UUID) value;
        ByteBuffer uuidBuf = ByteBuffer.allocate(16);
        uuidBuf.putLong(uuid.getMostSignificantBits());
        uuidBuf.putLong(uuid.getLeastSignificantBits());
        ((FixedSizeBinaryVector) batch.getVector(vectorName)).setSafe(rowIndex, uuidBuf.array());
        break;
      case FIXED:
        byte[] fixedBytes = toByteArray(value);
        ((FixedSizeBinaryVector) batch.getVector(vectorName)).setSafe(rowIndex, fixedBytes);
        break;
      case BINARY:
        byte[] binBytes = toByteArray(value);
        ((VarBinaryVector) batch.getVector(vectorName))
            .setSafe(rowIndex, binBytes, 0, binBytes.length);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported write type: " + field.type().typeId());
    }
  }

  private static byte[] toByteArray(Object value) {
    if (value instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) value;
      byte[] bytes = new byte[buf.remaining()];
      buf.duplicate().get(bytes);
      return bytes;
    }
    return (byte[]) value;
  }

  private static Object readValue(
      VectorSchemaRoot batch, String vectorName, int rowIndex, Types.NestedField field) {
    if (batch.getVector(vectorName).isNull(rowIndex)) {
      return null;
    }

    switch (field.type().typeId()) {
      case BOOLEAN:
        return ((BitVector) batch.getVector(vectorName)).get(rowIndex) == 1;
      case INTEGER:
        return ((IntVector) batch.getVector(vectorName)).get(rowIndex);
      case LONG:
        return ((BigIntVector) batch.getVector(vectorName)).get(rowIndex);
      case FLOAT:
        return ((Float4Vector) batch.getVector(vectorName)).get(rowIndex);
      case DOUBLE:
        return ((Float8Vector) batch.getVector(vectorName)).get(rowIndex);
      case DECIMAL:
        return ((DecimalVector) batch.getVector(vectorName)).getObject(rowIndex);
      default:
        return readTemporalOrBinaryValue(batch, vectorName, rowIndex, field);
    }
  }

  private static Object readTemporalOrBinaryValue(
      VectorSchemaRoot batch, String vectorName, int rowIndex, Types.NestedField field) {
    switch (field.type().typeId()) {
      case DATE:
        int days = ((DateDayVector) batch.getVector(vectorName)).get(rowIndex);
        return LocalDate.ofEpochDay(days);
      case TIME:
        long micros = ((TimeMicroVector) batch.getVector(vectorName)).get(rowIndex);
        return LocalTime.ofNanoOfDay(micros * 1000);
      case TIMESTAMP:
        long tsMicros;
        if (((Types.TimestampType) field.type()).shouldAdjustToUTC()) {
          tsMicros = ((TimeStampMicroTZVector) batch.getVector(vectorName)).get(rowIndex);
        } else {
          tsMicros = ((TimeStampMicroVector) batch.getVector(vectorName)).get(rowIndex);
        }
        return fromTimestampMicros(tsMicros);
      case STRING:
        return new String(
            ((VarCharVector) batch.getVector(vectorName)).get(rowIndex), StandardCharsets.UTF_8);
      case UUID:
        byte[] uuidBytes = ((FixedSizeBinaryVector) batch.getVector(vectorName)).get(rowIndex);
        ByteBuffer uuidBuf = ByteBuffer.wrap(uuidBytes);
        return new UUID(uuidBuf.getLong(), uuidBuf.getLong());
      case FIXED:
        return ByteBuffer.wrap(((FixedSizeBinaryVector) batch.getVector(vectorName)).get(rowIndex));
      case BINARY:
        return ByteBuffer.wrap(((VarBinaryVector) batch.getVector(vectorName)).get(rowIndex));
      default:
        throw new UnsupportedOperationException("Unsupported read type: " + field.type().typeId());
    }
  }

  private static long toTimestampMicros(Object value) {
    if (value instanceof OffsetDateTime) {
      OffsetDateTime odt = (OffsetDateTime) value;
      Instant instant = odt.toInstant();
      return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    } else if (value instanceof Long) {
      return (Long) value;
    } else {
      throw new UnsupportedOperationException("Cannot convert to timestamp micros: " + value);
    }
  }

  private static OffsetDateTime fromTimestampMicros(long micros) {
    long seconds = Math.floorDiv(micros, 1_000_000);
    long nanoAdjustment = Math.floorMod(micros, 1_000_000) * 1000L;
    return OffsetDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoAdjustment), ZoneOffset.UTC);
  }
}
