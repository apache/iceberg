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
package org.apache.iceberg.data.vortex;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.Serialized;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.vortex.VortexValueWriter;

/** Writes Iceberg generic {@link Record} objects to Arrow vectors for Vortex file output. */
public class GenericVortexWriter implements VortexValueWriter<Record> {
  private static final OffsetDateTime EPOCH =
      OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final LocalDateTime LOCAL_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);

  private final List<Types.NestedField> columns;
  private final ColumnMetricsTracker<?>[] trackers;

  private GenericVortexWriter(Schema schema) {
    this.columns = schema.columns();
    this.trackers = new ColumnMetricsTracker[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      trackers[i] = newTracker(columns.get(i));
    }
  }

  public static VortexValueWriter<Record> buildWriter(Schema schema) {
    return new GenericVortexWriter(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Record datum, VectorSchemaRoot root, int rowIndex) {
    for (int fieldIndex = 0; fieldIndex < columns.size(); fieldIndex++) {
      Types.NestedField field = columns.get(fieldIndex);
      FieldVector vector = root.getVector(fieldIndex);
      Object value = datum.get(fieldIndex);

      ColumnMetricsTracker<Object> tracker = (ColumnMetricsTracker<Object>) trackers[fieldIndex];
      if (value == null) {
        if (field.isRequired()) {
          throw new IllegalArgumentException(
              "Cannot write null value for required field: " + field);
        }

        writeNull(vector, field.type(), rowIndex);
        tracker.addNull();
        continue;
      }

      tracker.addValue(value);
      writeValue(vector, field.type(), value, rowIndex);
    }
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    Stream.Builder<FieldMetrics<?>> builder = Stream.builder();
    for (int i = 0; i < columns.size(); i++) {
      builder.add(trackers[i].toFieldMetrics());
    }
    return builder.build();
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static void writeValue(
      FieldVector vector, org.apache.iceberg.types.Type type, Object value, int rowIndex) {
    switch (type.typeId()) {
      case BOOLEAN:
        ((BitVector) vector).setSafe(rowIndex, ((Boolean) value) ? 1 : 0);
        break;
      case INTEGER:
        ((IntVector) vector).setSafe(rowIndex, (Integer) value);
        break;
      case LONG:
        ((BigIntVector) vector).setSafe(rowIndex, (Long) value);
        break;
      case FLOAT:
        ((Float4Vector) vector).setSafe(rowIndex, (Float) value);
        break;
      case DOUBLE:
        ((Float8Vector) vector).setSafe(rowIndex, (Double) value);
        break;
      case STRING:
        byte[] strBytes = value.toString().getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) vector).setSafe(rowIndex, strBytes);
        break;
      case BINARY:
        byte[] binaryBytes;
        if (value instanceof ByteBuffer) {
          binaryBytes = ByteBuffers.toByteArray((ByteBuffer) value);
        } else {
          binaryBytes = (byte[]) value;
        }

        ((VarBinaryVector) vector).setSafe(rowIndex, binaryBytes);
        break;
      case FIXED:
        // FIXED maps to Arrow FixedSizeBinaryVector, not VarBinaryVector. Until the writer is
        // updated to use FixedSizeBinaryVector and validate the byte length, refuse the write
        // rather than failing with a cryptic ClassCastException at runtime.
        throw new UnsupportedOperationException(
            "Writing Iceberg FIXED columns to Vortex is not yet supported");
      case DECIMAL:
        ((DecimalVector) vector).setSafe(rowIndex, (BigDecimal) value);
        break;
      case DATE:
        int epochDay = (int) ((LocalDate) value).toEpochDay();
        ((DateDayVector) vector).setSafe(rowIndex, epochDay);
        break;
      case UUID:
        FixedSizeBinaryVector uuidStorage =
            vector instanceof ExtensionTypeVector<?> ext
                ? (FixedSizeBinaryVector) ext.getUnderlyingVector()
                : (FixedSizeBinaryVector) vector;
        uuidStorage.setSafe(rowIndex, UUIDUtil.convert((UUID) value));
        break;
      case TIME:
        long timeMicros = ((LocalTime) value).getLong(java.time.temporal.ChronoField.MICRO_OF_DAY);
        ((TimeMicroVector) vector).setSafe(rowIndex, timeMicros);
        break;
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          long epochMicros = ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) value);
          ((TimeStampMicroTZVector) vector).setSafe(rowIndex, epochMicros);
        } else {
          long localEpochMicros = ChronoUnit.MICROS.between(LOCAL_EPOCH, (LocalDateTime) value);
          ((TimeStampMicroVector) vector).setSafe(rowIndex, localEpochMicros);
        }

        break;
      case TIMESTAMP_NANO:
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) type;
        if (tsNanoType.shouldAdjustToUTC()) {
          long epochNanos = ChronoUnit.NANOS.between(EPOCH, (OffsetDateTime) value);
          ((TimeStampNanoTZVector) vector).setSafe(rowIndex, epochNanos);
        } else {
          long localEpochNanos = ChronoUnit.NANOS.between(LOCAL_EPOCH, (LocalDateTime) value);
          ((TimeStampNanoVector) vector).setSafe(rowIndex, localEpochNanos);
        }

        break;
      case LIST:
        Types.ListType listType = (Types.ListType) type;
        org.apache.iceberg.types.Type elementType = listType.elementType();
        ListVector listVector = (ListVector) vector;
        FieldVector elementVector = listVector.getDataVector();
        List<?> elements = (List<?>) value;
        int elementStart = listVector.startNewValue(rowIndex);
        for (int i = 0; i < elements.size(); i++) {
          Object elementValue = elements.get(i);
          int elementIdx = elementStart + i;
          if (elementValue == null) {
            elementVector.setNull(elementIdx);
          } else {
            writeValue(elementVector, elementType, elementValue, elementIdx);
          }
        }
        listVector.endValue(rowIndex, elements.size());
        break;
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        StructVector structVector = (StructVector) vector;
        Record structValue = (Record) value;
        List<Types.NestedField> structFields = structType.fields();
        for (int i = 0; i < structFields.size(); i++) {
          Types.NestedField structField = structFields.get(i);
          // Bind each Iceberg child to the Arrow child of the same name; the Arrow struct is built
          // from the write schema, so names line up even if ordinals were to drift.
          FieldVector childVector = (FieldVector) structVector.getChild(structField.name());
          Object childValue = structValue.get(i);
          if (childValue == null) {
            childVector.setNull(rowIndex);
          } else {
            writeValue(childVector, structField.type(), childValue, rowIndex);
          }
        }
        // Mark the struct slot itself as non-null for this row.
        structVector.setIndexDefined(rowIndex);
        break;
      case VARIANT:
        writeVariant((StructVector) vector, (Variant) value, rowIndex);

        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported Iceberg type for Vortex write: " + type);
    }
  }

  private static void writeNull(FieldVector vector, Type type, int rowIndex) {
    if (type.isVariantType()) {
      writeNullVariant((StructVector) vector, rowIndex);
    } else {
      vector.setNull(rowIndex);
    }
  }

  private static void writeNullVariant(StructVector vector, int rowIndex) {
    vector.setNull(rowIndex);
    writeVariantMetadata(
        vector.getChild("metadata", VarBinaryVector.class), VariantMetadata.empty(), rowIndex);

    VarBinaryVector valueVector = vector.getChild("value", VarBinaryVector.class);
    if (valueVector != null) {
      valueVector.setNull(rowIndex);
    }
  }

  private static void writeVariant(StructVector vector, Variant variant, int rowIndex) {
    vector.setIndexDefined(rowIndex);

    writeVariantMetadata(
        vector.getChild("metadata", VarBinaryVector.class), variant.metadata(), rowIndex);
    writeVariantValue(vector.getChild("value", VarBinaryVector.class), variant.value(), rowIndex);
  }

  private static void writeVariantMetadata(
      VarBinaryVector vector, VariantMetadata metadata, int rowIndex) {
    if (metadata instanceof Serialized serialized) {
      writeSerialized(vector, serialized, rowIndex);
      return;
    }

    ByteBuffer buffer = ByteBuffer.allocate(metadata.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    int length = metadata.writeTo(buffer, 0);
    vector.setSafe(rowIndex, buffer, 0, length);
  }

  private static void writeVariantValue(VarBinaryVector vector, VariantValue value, int rowIndex) {
    if (value instanceof Serialized serialized) {
      writeSerialized(vector, serialized, rowIndex);
      return;
    }

    ByteBuffer buffer = ByteBuffer.allocate(value.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    int length = value.writeTo(buffer, 0);
    vector.setSafe(rowIndex, buffer, 0, length);
  }

  private static void writeSerialized(VarBinaryVector vector, Serialized serialized, int rowIndex) {
    vector.setSafe(rowIndex, ByteBuffers.toByteArray(serialized.buffer()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ColumnMetricsTracker<?> newTracker(Types.NestedField field) {
    switch (field.type().typeId()) {
      case FLOAT:
        return new FloatMetricsTracker(field.fieldId());
      case DOUBLE:
        return new DoubleMetricsTracker(field.fieldId());
      case DATE:
        return new ColumnMetricsTracker<Integer>(
            field.fieldId(), Comparator.naturalOrder(), v -> (int) ((LocalDate) v).toEpochDay());
      case TIME:
        return new ColumnMetricsTracker<Long>(
            field.fieldId(),
            Comparator.naturalOrder(),
            v -> ((LocalTime) v).getLong(java.time.temporal.ChronoField.MICRO_OF_DAY));
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) field.type();
        if (tsType.shouldAdjustToUTC()) {
          return new ColumnMetricsTracker<Long>(
              field.fieldId(),
              Comparator.naturalOrder(),
              v -> ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) v));
        } else {
          return new ColumnMetricsTracker<Long>(
              field.fieldId(),
              Comparator.naturalOrder(),
              v -> ChronoUnit.MICROS.between(LOCAL_EPOCH, (LocalDateTime) v));
        }
      case TIMESTAMP_NANO:
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) field.type();
        if (tsNanoType.shouldAdjustToUTC()) {
          return new ColumnMetricsTracker<Long>(
              field.fieldId(),
              Comparator.naturalOrder(),
              v -> ChronoUnit.NANOS.between(EPOCH, (OffsetDateTime) v));
        } else {
          return new ColumnMetricsTracker<Long>(
              field.fieldId(),
              Comparator.naturalOrder(),
              v -> ChronoUnit.NANOS.between(LOCAL_EPOCH, (LocalDateTime) v));
        }
      default:
        if (field.type().isNestedType() || field.type().isVariantType()) {
          // Lists, maps, and structs have no natural ordering — track counts only.
          return new ColumnMetricsTracker<>(field.fieldId(), null);
        }
        return new ColumnMetricsTracker<>(field.fieldId(), (Comparator) Comparator.naturalOrder());
    }
  }

  /**
   * Tracks per-column metrics during writes: value count, null count, and min/max bounds using
   * natural ordering. An optional converter transforms values to their internal representation
   * (e.g., LocalDateTime to Long microseconds) before tracking bounds.
   */
  static class ColumnMetricsTracker<T> {
    private final int fieldId;
    private final Comparator<T> comparator;
    private final java.util.function.Function<Object, T> converter;
    private long valueCount;
    private long nullCount;
    private T min;
    private T max;

    ColumnMetricsTracker(int fieldId) {
      this(fieldId, null, null);
    }

    ColumnMetricsTracker(int fieldId, Comparator<T> comparator) {
      this(fieldId, comparator, null);
    }

    @SuppressWarnings("unchecked")
    ColumnMetricsTracker(
        int fieldId, Comparator<T> comparator, java.util.function.Function<Object, T> converter) {
      this.fieldId = fieldId;
      this.comparator = comparator;
      this.converter = converter;
    }

    void addNull() {
      valueCount++;
      nullCount++;
    }

    void incrementValueCount() {
      valueCount++;
    }

    @SuppressWarnings("unchecked")
    void addValue(Object value) {
      valueCount++;
      if (comparator == null) {
        return;
      }
      T typedValue = converter != null ? converter.apply(value) : (T) value;
      if (min == null || comparator.compare(typedValue, min) < 0) {
        min = typedValue;
      }
      if (max == null || comparator.compare(typedValue, max) > 0) {
        max = typedValue;
      }
    }

    FieldMetrics<?> toFieldMetrics() {
      return new FieldMetrics<>(fieldId, valueCount, nullCount, nanValueCount(), min, max);
    }

    long nanValueCount() {
      return -1;
    }
  }

  /** Float-specific tracker that handles NaN values by excluding them from bounds. */
  static class FloatMetricsTracker extends ColumnMetricsTracker<Float> {
    private long nanCount;

    FloatMetricsTracker(int fieldId) {
      super(fieldId, Comparator.naturalOrder());
    }

    @Override
    void addValue(Object value) {
      if (Float.isNaN((Float) value)) {
        incrementValueCount();
        nanCount++;
      } else {
        super.addValue(value);
      }
    }

    @Override
    long nanValueCount() {
      return nanCount;
    }
  }

  /** Double-specific tracker that handles NaN values by excluding them from bounds. */
  static class DoubleMetricsTracker extends ColumnMetricsTracker<Double> {
    private long nanCount;

    DoubleMetricsTracker(int fieldId) {
      super(fieldId, Comparator.naturalOrder());
    }

    @Override
    void addValue(Object value) {
      if (Double.isNaN((Double) value)) {
        incrementValueCount();
        nanCount++;
      } else {
        super.addValue(value);
      }
    }

    @Override
    long nanValueCount() {
      return nanCount;
    }
  }
}
