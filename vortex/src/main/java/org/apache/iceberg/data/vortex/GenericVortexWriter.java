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
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.Serialized;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.vortex.VortexSchemas;
import org.apache.iceberg.vortex.VortexValueWriter;

/** Writes Iceberg generic {@link Record} objects to Arrow vectors for Vortex file output. */
public class GenericVortexWriter implements VortexValueWriter<Record> {
  private static final OffsetDateTime EPOCH =
      OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final LocalDateTime LOCAL_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);

  private final List<Types.NestedField> columns;

  // Unknown columns are not written to the file at all (see VortexSchemas#writtenFields), so the
  // Arrow root holds fewer vectors than the schema has columns. Maps each column to its vector,
  // with -1 for the unknown columns that have none.
  private final int[] vectorIndex;

  private GenericVortexWriter(Schema schema) {
    this.columns = schema.columns();
    this.vectorIndex = new int[columns.size()];
    int nextVector = 0;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).type().typeId() == Type.TypeID.UNKNOWN) {
        vectorIndex[i] = -1;
      } else {
        vectorIndex[i] = nextVector;
        nextVector += 1;
      }
    }
  }

  public static VortexValueWriter<Record> buildWriter(Schema schema) {
    return new GenericVortexWriter(schema);
  }

  @Override
  public void write(Record datum, VectorSchemaRoot root, int rowIndex) {
    for (int fieldIndex = 0; fieldIndex < columns.size(); fieldIndex++) {
      if (vectorIndex[fieldIndex] < 0) {
        // An unknown column holds nothing but nulls and is not stored.
        continue;
      }

      Types.NestedField field = columns.get(fieldIndex);
      FieldVector vector = root.getVector(vectorIndex[fieldIndex]);
      Object value = datum.get(fieldIndex);

      if (value == null) {
        if (field.isRequired()) {
          throw new IllegalArgumentException(
              "Cannot write null value for required field: " + field);
        }

        writeNull(vector, field.type(), rowIndex);
        continue;
      }

      writeValue(vector, field.type(), value, rowIndex);
    }
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static void writeValue(
      FieldVector vector, org.apache.iceberg.types.Type type, Object value, int rowIndex) {
    switch (type.typeId()) {
      case UNKNOWN:
        // Unreachable: Iceberg requires unknown fields to be optional, and every value is null, so
        // writes go through writeNull. Kept so the switch stays total.
        throw new IllegalArgumentException("Cannot write a non-null value for unknown: " + value);
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
      case GEOMETRY:
      case GEOGRAPHY:
        // Geometry and geography are WKB, written verbatim as binary (see VortexSchemas).
        byte[] binaryBytes;
        if (value instanceof ByteBuffer buffer) {
          binaryBytes = ByteBuffers.toByteArray(buffer);
        } else {
          binaryBytes = (byte[]) value;
        }

        ((VarBinaryVector) vector).setSafe(rowIndex, binaryBytes);
        break;
      case FIXED:
        // Unreachable in practice: VortexSchemas refuses FIXED while building the file schema,
        // because Vortex has no fixed-width binary type. Kept so the switch stays total.
        throw new UnsupportedOperationException(
            "Cannot write Iceberg FIXED column: Vortex has no fixed-width binary type");
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
          if (structField.type().typeId() == Type.TypeID.UNKNOWN) {
            // Not stored, so the Arrow struct has no child to write it to.
            continue;
          }

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
      case MAP:
        writeMap((MapVector) vector, (Types.MapType) type, (Map<?, ?>) value, rowIndex);
        break;
      case VARIANT:
        writeVariant((StructVector) vector, (Variant) value, rowIndex);

        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported Iceberg type for Vortex write: " + type);
    }
  }

  /**
   * Writes a map value into Arrow's map layout: a list of non-nullable {@code entries} structs
   * holding {@code key} and {@code value} children. Entries are appended at the row's offset in the
   * shared child vectors, mirroring how list elements are written.
   */
  private static void writeMap(
      MapVector vector, Types.MapType mapType, Map<?, ?> value, int rowIndex) {
    StructVector entries = (StructVector) vector.getDataVector();
    FieldVector keyVector = entries.getChild(VortexSchemas.MAP_KEY_NAME, FieldVector.class);
    FieldVector valueVector = entries.getChild(VortexSchemas.MAP_VALUE_NAME, FieldVector.class);

    int entryStart = vector.startNewValue(rowIndex);
    int offset = 0;
    for (Map.Entry<?, ?> entry : value.entrySet()) {
      int entryIndex = entryStart + offset;
      entries.setIndexDefined(entryIndex);

      Preconditions.checkArgument(entry.getKey() != null, "Cannot write null map key");
      writeValue(keyVector, mapType.keyType(), entry.getKey(), entryIndex);

      if (entry.getValue() == null) {
        Preconditions.checkArgument(
            mapType.isValueOptional(), "Cannot write null value for required map value type");
        writeNull(valueVector, mapType.valueType(), entryIndex);
      } else {
        writeValue(valueVector, mapType.valueType(), entry.getValue(), entryIndex);
      }

      offset += 1;
    }

    vector.endValue(rowIndex, offset);
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
}
