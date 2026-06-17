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
package org.apache.iceberg.spark.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.vortex.VortexValueWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/** Writes Spark {@link InternalRow} objects to Arrow vectors for Vortex file output. */
public class SparkVortexWriter implements VortexValueWriter<InternalRow> {
  private final List<Types.NestedField> columns;

  public SparkVortexWriter(Schema schema) {
    this.columns = schema.columns();
  }

  public static VortexValueWriter<InternalRow> buildWriter(Schema schema) {
    return new SparkVortexWriter(schema);
  }

  @Override
  public void write(InternalRow datum, VectorSchemaRoot root, int rowIndex) {
    for (int fieldIndex = 0; fieldIndex < columns.size(); fieldIndex++) {
      Types.NestedField field = columns.get(fieldIndex);
      FieldVector vector = root.getVector(fieldIndex);

      if (field.isOptional() && datum.isNullAt(fieldIndex)) {
        writeNull(vector, field.type(), rowIndex);
        continue;
      }

      writeValue(vector, field.type(), datum, fieldIndex, rowIndex);
    }
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static void writeValue(
      FieldVector vector,
      org.apache.iceberg.types.Type type,
      InternalRow row,
      int fieldIndex,
      int rowIndex) {
    switch (type.typeId()) {
      case BOOLEAN:
        ((BitVector) vector).setSafe(rowIndex, row.getBoolean(fieldIndex) ? 1 : 0);
        break;
      case INTEGER:
        ((IntVector) vector).setSafe(rowIndex, row.getInt(fieldIndex));
        break;
      case LONG:
        ((BigIntVector) vector).setSafe(rowIndex, row.getLong(fieldIndex));
        break;
      case FLOAT:
        ((Float4Vector) vector).setSafe(rowIndex, row.getFloat(fieldIndex));
        break;
      case DOUBLE:
        ((Float8Vector) vector).setSafe(rowIndex, row.getDouble(fieldIndex));
        break;
      case STRING:
        UTF8String str = row.getUTF8String(fieldIndex);
        ((VarCharVector) vector).setSafe(rowIndex, str.getBytes());
        break;
      case BINARY:
        byte[] bytes = row.getBinary(fieldIndex);
        ((VarBinaryVector) vector).setSafe(rowIndex, bytes);
        break;
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        BigDecimal decimal =
            row.getDecimal(fieldIndex, decimalType.precision(), decimalType.scale())
                .toJavaBigDecimal();
        ((DecimalVector) vector).setSafe(rowIndex, decimal);
        break;
      case DATE:
        ((DateDayVector) vector).setSafe(rowIndex, row.getInt(fieldIndex));
        break;
      case UUID:
        UUID uuid = UUID.fromString(row.getUTF8String(fieldIndex).toString());
        FixedSizeBinaryVector uuidStorage =
            vector instanceof ExtensionTypeVector<?> ext
                ? (FixedSizeBinaryVector) ext.getUnderlyingVector()
                : (FixedSizeBinaryVector) vector;
        uuidStorage.setSafe(rowIndex, UUIDUtil.convert(uuid));
        break;
      case TIME:
        ((TimeMicroVector) vector).setSafe(rowIndex, row.getLong(fieldIndex));
        break;
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          ((TimeStampMicroTZVector) vector).setSafe(rowIndex, row.getLong(fieldIndex));
        } else {
          ((TimeStampMicroVector) vector).setSafe(rowIndex, row.getLong(fieldIndex));
        }

        break;
      case TIMESTAMP_NANO:
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) type;
        if (tsNanoType.shouldAdjustToUTC()) {
          ((TimeStampNanoTZVector) vector).setSafe(rowIndex, row.getLong(fieldIndex));
        } else {
          ((TimeStampNanoVector) vector).setSafe(rowIndex, row.getLong(fieldIndex));
        }

        break;
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        StructVector structVector = (StructVector) vector;
        List<Types.NestedField> structFields = structType.fields();
        InternalRow structRow = row.getStruct(fieldIndex, structFields.size());
        for (int i = 0; i < structFields.size(); i++) {
          Types.NestedField structField = structFields.get(i);
          // Bind each Iceberg child to the Arrow child of the same name; the Arrow struct is built
          // from the write schema, so names line up even if ordinals were to drift.
          FieldVector childVector = (FieldVector) structVector.getChild(structField.name());
          if (structRow.isNullAt(i)) {
            childVector.setNull(rowIndex);
          } else {
            writeValue(childVector, structField.type(), structRow, i, rowIndex);
          }
        }
        // Mark the struct slot itself as non-null for this row.
        structVector.setIndexDefined(rowIndex);
        break;
      case VARIANT:
        writeVariant((StructVector) vector, row.getVariant(fieldIndex), rowIndex);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported Iceberg type for Vortex write: " + type);
    }
  }

  private static void writeNull(
      FieldVector vector, org.apache.iceberg.types.Type type, int rowIndex) {
    if (type.isVariantType()) {
      writeNullVariant((StructVector) vector, rowIndex);
    } else {
      vector.setNull(rowIndex);
    }
  }

  // Variant storage is an Arrow struct of { required metadata, optional value }. Spark's VariantVal
  // already holds the serialized little-endian metadata and value, so copy them straight across.
  private static void writeVariant(StructVector vector, VariantVal variant, int rowIndex) {
    vector.getChild("metadata", VarBinaryVector.class).setSafe(rowIndex, variant.getMetadata());
    vector.getChild("value", VarBinaryVector.class).setSafe(rowIndex, variant.getValue());
    vector.setIndexDefined(rowIndex);
  }

  // The metadata child is required, so a null variant still needs a valid (empty) metadata entry.
  private static void writeNullVariant(StructVector vector, int rowIndex) {
    vector.setNull(rowIndex);

    VariantMetadata empty = VariantMetadata.empty();
    byte[] metadataBytes = new byte[empty.sizeInBytes()];
    empty.writeTo(ByteBuffer.wrap(metadataBytes).order(ByteOrder.LITTLE_ENDIAN), 0);
    vector.getChild("metadata", VarBinaryVector.class).setSafe(rowIndex, metadataBytes);

    VarBinaryVector valueVector = vector.getChild("value", VarBinaryVector.class);
    if (valueVector != null) {
      valueVector.setNull(rowIndex);
    }
  }
}
