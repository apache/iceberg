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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericDataUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.iceberg.vortex.VortexSchemaWithTypeVisitor;
import org.apache.iceberg.vortex.VortexSchemas;
import org.apache.iceberg.vortex.VortexValueReader;

public class GenericVortexReader implements VortexRowReader<Record> {
  private final Types.StructType structType;

  // Parallel arrays indexed by position in the expected (projected) struct. A field is read either
  // from a file column ({@code columnNames[i]} is the Arrow column name) or as a constant
  // ({@code columnNames[i]} is null and {@code readers[i]} is a constant reader).
  private final VortexValueReader<?>[] readers;
  private final String[] columnNames;

  // Resolves expected field position -> Arrow batch column index. Vortex only returns the projected
  // (non-constant, file-resident) columns, so this mapping is computed by name from the first batch
  // rather than assuming the batch is positionally aligned with the expected schema. -1 marks a
  // constant field that is not backed by a batch column.
  private int[] batchColumnIndex;

  private boolean reuseContainers = false;
  private GenericRecord reusedRecord = null;

  private GenericVortexReader(
      Schema expectedSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      Map<Integer, ?> idToConstant) {
    this.structType = expectedSchema.asStruct();
    Map<Integer, ?> constants = idToConstant == null ? Collections.emptyMap() : idToConstant;

    List<Field> fileFields = fileArrowSchema.getFields();
    Map<String, Field> arrowFieldsByName = Maps.newHashMapWithExpectedSize(fileFields.size());
    for (Field field : fileFields) {
      arrowFieldsByName.put(field.getName(), field);
    }

    GenericReadBuilder builder = new GenericReadBuilder();
    List<Types.NestedField> expectedFields = structType.fields();
    this.readers = new VortexValueReader<?>[expectedFields.size()];
    this.columnNames = new String[expectedFields.size()];

    for (int i = 0; i < expectedFields.size(); i++) {
      Types.NestedField field = expectedFields.get(i);
      int id = field.fieldId();
      if (initLineageReader(i, field, constants, arrowFieldsByName)) {
        continue;
      }

      if (constants.containsKey(id)) {
        // Identity-partition value or metadata column (e.g. _file, _spec_id, _partition) supplied
        // through idToConstant instead of being stored in the data file.
        this.readers[i] = GenericVortexReaders.constants(constants.get(id));
      } else if (id == MetadataColumns.IS_DELETED.fieldId()) {
        this.readers[i] = GenericVortexReaders.constants(false);
      } else if (id == MetadataColumns.ROW_POSITION.fieldId()) {
        this.readers[i] = GenericVortexReaders.longs();
        this.columnNames[i] = field.name();
      } else {
        Field arrowField = arrowFieldsByName.get(field.name());
        if (arrowField == null) {
          if (field.initialDefault() != null) {
            this.readers[i] =
                GenericVortexReaders.constants(
                    GenericDataUtil.internalToGeneric(field.type(), field.initialDefault()));
          } else if (field.isOptional()) {
            // The expected field is neither a constant nor present in the data file (for example an
            // unsupplied metadata column). Fill it with null rather than reading a missing column.
            this.readers[i] = GenericVortexReaders.constants(null);
          } else {
            throw new IllegalArgumentException(
                String.format("Missing required field: %s", field.name()));
          }
        } else {
          this.readers[i] = VortexSchemaWithTypeVisitor.visit(field.type(), arrowField, builder);
          this.columnNames[i] = field.name();
        }
      }
    }
  }

  /**
   * Wires the row lineage metadata columns when the engine supplies inheritance bases through
   * {@code idToConstant}: {@code _row_id} binds to the {@code {value?, pos}} struct packed by the
   * scan (stored values win, nulls inherit {@code firstRowId + position}) and {@code
   * _last_updated_sequence_number} substitutes the file's sequence number for null or absent
   * values. Returns whether the field was handled.
   */
  private boolean initLineageReader(
      int pos,
      Types.NestedField field,
      Map<Integer, ?> constants,
      Map<String, Field> arrowFieldsByName) {
    int id = field.fieldId();
    if (id == MetadataColumns.ROW_ID.fieldId() && constants.get(id) instanceof Long firstRowId) {
      this.readers[pos] = GenericVortexReaders.rowIds(firstRowId);
      this.columnNames[pos] = field.name();
      return true;
    }

    if (id == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()
        && constants.get(id) instanceof Long seqNumber) {
      if (arrowFieldsByName.containsKey(field.name())) {
        this.readers[pos] = GenericVortexReaders.longsOrDefault(seqNumber);
        this.columnNames[pos] = field.name();
      } else {
        this.readers[pos] = GenericVortexReaders.constants(seqNumber);
      }

      return true;
    }

    return false;
  }

  public static VortexRowReader<Record> buildReader(
      Schema expectedSchema, org.apache.arrow.vector.types.pojo.Schema fileArrowSchema) {
    return new GenericVortexReader(expectedSchema, fileArrowSchema, Collections.emptyMap());
  }

  public static VortexRowReader<Record> buildReader(
      Schema expectedSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      Map<Integer, ?> idToConstant) {
    return new GenericVortexReader(expectedSchema, fileArrowSchema, idToConstant);
  }

  @Override
  public void reuseContainers(boolean reuse) {
    this.reuseContainers = reuse;
  }

  @Override
  public Record read(VectorSchemaRoot batch, int row) {
    if (batchColumnIndex == null) {
      this.batchColumnIndex = resolveColumns(batch);
    }

    GenericRecord record = container();
    for (int i = 0; i < readers.length; i++) {
      int columnIndex = batchColumnIndex[i];
      FieldVector vector = columnIndex < 0 ? null : batch.getVector(columnIndex);
      record.set(i, readers[i].read(vector, row));
    }
    return record;
  }

  private GenericRecord container() {
    if (!reuseContainers) {
      return GenericRecord.create(structType);
    }

    if (reusedRecord == null) {
      this.reusedRecord = GenericRecord.create(structType);
    }

    return reusedRecord;
  }

  private int[] resolveColumns(VectorSchemaRoot batch) {
    List<FieldVector> vectors = batch.getFieldVectors();
    Map<String, Integer> nameToIndex = Maps.newHashMapWithExpectedSize(vectors.size());
    for (int i = 0; i < vectors.size(); i++) {
      nameToIndex.put(vectors.get(i).getField().getName(), i);
    }

    int[] indexes = new int[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      if (columnNames[i] == null) {
        indexes[i] = -1;
      } else {
        Integer index = nameToIndex.get(columnNames[i]);
        Preconditions.checkState(
            index != null, "Vortex batch is missing projected column: %s", columnNames[i]);
        indexes[i] = index;
      }
    }

    return indexes;
  }

  static class GenericReadBuilder extends VortexSchemaWithTypeVisitor<VortexValueReader<?>> {
    GenericReadBuilder() {}

    @Override
    public VortexValueReader<?> struct(
        Types.StructType iStruct, List<Field> fields, List<VortexValueReader<?>> children) {
      return GenericVortexReaders.struct(iStruct, fields, children);
    }

    @Override
    public VortexValueReader<?> list(
        Types.ListType iList, Field listField, VortexValueReader<?> element) {
      return GenericVortexReaders.list(element);
    }

    @Override
    public VortexValueReader<?> primitive(Type.PrimitiveType iPrimitive, Field primField) {
      if ((iPrimitive != null && iPrimitive.typeId() == Type.TypeID.UUID)
          || VortexSchemas.isUuidField(primField)) {
        return GenericVortexReaders.uuids();
      }
      ArrowType arrowType = primField.getType();
      if (arrowType instanceof ArrowType.Int intType) {
        if (intType.getBitWidth() > Integer.SIZE) {
          return GenericVortexReaders.longs();
        }
        // Schema evolution: an int column may be projected as long after a type promotion.
        return iPrimitive != null && iPrimitive.typeId() == Type.TypeID.LONG
            ? GenericVortexReaders.intsAsLongs()
            : GenericVortexReaders.ints();
      } else if (arrowType instanceof ArrowType.FloatingPoint fpType) {
        return floatingPointReader(fpType, iPrimitive);
      } else if (arrowType instanceof ArrowType.Date dateType) {
        return GenericVortexReaders.date(dateType.getUnit() == DateUnit.MILLISECOND);
      } else if (arrowType instanceof ArrowType.Time timeType) {
        return GenericVortexReaders.time(timeType.getUnit() == TimeUnit.NANOSECOND);
      } else if (arrowType instanceof ArrowType.Timestamp tsType) {
        return timestampReader(tsType);
      }
      return simpleReader(arrowType);
    }

    @Override
    public VortexValueReader<?> variant(Types.VariantType variantType, Field variantField) {
      return GenericVortexReaders.variants();
    }

    private static VortexValueReader<?> simpleReader(ArrowType arrowType) {
      if (arrowType instanceof ArrowType.Bool) {
        return GenericVortexReaders.bools();
      } else if (arrowType instanceof ArrowType.Decimal) {
        return GenericVortexReaders.decimals();
      } else if (arrowType instanceof ArrowType.Utf8View) {
        return GenericVortexReaders.stringsView();
      } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
        return GenericVortexReaders.strings();
      } else if (arrowType instanceof ArrowType.BinaryView) {
        return GenericVortexReaders.bytesView();
      } else if (arrowType instanceof ArrowType.Binary
          || arrowType instanceof ArrowType.LargeBinary
          || arrowType instanceof ArrowType.FixedSizeBinary) {
        return GenericVortexReaders.bytes();
      }
      throw new UnsupportedOperationException(
          "Unsupported Arrow type in Vortex read: " + arrowType);
    }

    private static VortexValueReader<?> floatingPointReader(
        ArrowType.FloatingPoint fpType, Type.PrimitiveType iPrimitive) {
      return switch (fpType.getPrecision()) {
          // Schema evolution: a float column may be projected as double after a type promotion.
        case SINGLE ->
            iPrimitive != null && iPrimitive.typeId() == Type.TypeID.DOUBLE
                ? GenericVortexReaders.floatsAsDoubles()
                : GenericVortexReaders.floats();
        case DOUBLE -> GenericVortexReaders.doubles();
        case HALF ->
            throw new UnsupportedOperationException("Half-precision floats are not supported");
      };
    }

    private static VortexValueReader<?> timestampReader(ArrowType.Timestamp tsType) {
      boolean isNano = tsType.getUnit() == TimeUnit.NANOSECOND;
      if (tsType.getTimezone() == null) {
        return GenericVortexReaders.timestamp(isNano);
      }
      return GenericVortexReaders.timestampTz(tsType.getTimezone(), isNano);
    }
  }
}
