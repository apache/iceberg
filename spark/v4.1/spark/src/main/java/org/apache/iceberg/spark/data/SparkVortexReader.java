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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.vortex.GenericVortexReaders;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.iceberg.vortex.VortexSchemaWithTypeVisitor;
import org.apache.iceberg.vortex.VortexValueReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/** Read Vortex as Spark {@link InternalRow}. */
public class SparkVortexReader implements VortexRowReader<InternalRow> {

  // Parallel arrays indexed by position in the expected (projected) schema. A field is read either
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
  private GenericInternalRow reusedRow = null;

  public SparkVortexReader(
      Schema readSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      Map<Integer, ?> idToConstant) {
    Map<Integer, ?> constants = idToConstant == null ? Collections.emptyMap() : idToConstant;

    List<Field> fileFields = fileArrowSchema.getFields();
    Map<String, Field> arrowFieldsByName = Maps.newHashMapWithExpectedSize(fileFields.size());
    for (Field field : fileFields) {
      arrowFieldsByName.put(field.getName(), field);
    }

    List<Types.NestedField> expected = readSchema.columns();
    this.readers = new VortexValueReader<?>[expected.size()];
    this.columnNames = new String[expected.size()];

    for (int i = 0; i < expected.size(); i++) {
      Types.NestedField field = expected.get(i);
      int id = field.fieldId();
      if (id == MetadataColumns.ROW_ID.fieldId() && constants.get(id) instanceof Long firstRowId) {
        // Row lineage: the scan packs {value: stored _row_id (when present), pos: row_idx} under
        // the _row_id name; stored values win and nulls inherit firstRowId + position.
        this.readers[i] = GenericVortexReaders.rowIds(firstRowId);
        this.columnNames[i] = field.name();
      } else if (id == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()
          && constants.get(id) instanceof Long seqNumber) {
        if (arrowFieldsByName.containsKey(field.name())) {
          // Stored values win; nulls inherit the file's sequence number.
          this.readers[i] = GenericVortexReaders.longsOrDefault(seqNumber);
          this.columnNames[i] = field.name();
        } else {
          this.readers[i] = GenericVortexReaders.constants(seqNumber);
        }
      } else if (constants.containsKey(id)) {
        // Identity-partition value or metadata column (e.g. _file, _spec_id, _partition) supplied
        // through idToConstant instead of being stored in the data file.
        this.readers[i] = GenericVortexReaders.constants(constants.get(id));
      } else if (id == MetadataColumns.IS_DELETED.fieldId()) {
        this.readers[i] = GenericVortexReaders.constants(false);
      } else {
        Field arrowField = arrowFieldsByName.get(field.name());
        if (arrowField == null) {
          this.readers[i] = defaultReader(field);
        } else {
          this.readers[i] =
              VortexSchemaWithTypeVisitor.visit(
                  field.type(), arrowField, SparkReadBuilder.INSTANCE);
          this.columnNames[i] = arrowField.getName();
        }
      }
    }
  }

  private static VortexValueReader<?> defaultReader(Types.NestedField field) {
    if (field.initialDefault() != null) {
      return GenericVortexReaders.constants(
          SparkUtil.internalToSpark(field.type(), field.initialDefault()));
    } else if (field.isOptional()) {
      return GenericVortexReaders.constants(null);
    }

    throw new IllegalArgumentException(String.format("Missing required field: %s", field.name()));
  }

  @Override
  public void reuseContainers(boolean reuse) {
    this.reuseContainers = reuse;
  }

  @Override
  public InternalRow read(VectorSchemaRoot batch, int row) {
    if (batchColumnIndex == null) {
      this.batchColumnIndex = resolveColumns(batch);
    }

    GenericInternalRow result = container();
    for (int i = 0; i < readers.length; i++) {
      int columnIndex = batchColumnIndex[i];
      FieldVector vector = columnIndex < 0 ? null : batch.getVector(columnIndex);
      result.update(i, readers[i].read(vector, row));
    }
    return result;
  }

  private GenericInternalRow container() {
    if (!reuseContainers) {
      return new GenericInternalRow(readers.length);
    }

    if (reusedRow == null) {
      this.reusedRow = new GenericInternalRow(readers.length);
    }

    return reusedRow;
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

  static class SparkReadBuilder extends VortexSchemaWithTypeVisitor<VortexValueReader<?>> {
    static final SparkReadBuilder INSTANCE = new SparkReadBuilder();

    private SparkReadBuilder() {}

    @Override
    public VortexValueReader<?> struct(
        Types.StructType schema, List<Field> fields, List<VortexValueReader<?>> children) {
      return new StructReader(schema, fields, children);
    }

    @Override
    public VortexValueReader<?> list(
        Types.ListType iList, Field listField, VortexValueReader<?> element) {
      return SparkVortexValueReaders.list(element);
    }

    @Override
    public VortexValueReader<?> variant(Types.VariantType variantType, Field variantField) {
      return SparkVortexValueReaders.variants();
    }

    @Override
    public VortexValueReader<?> primitive(Type.PrimitiveType icebergType, Field primField) {
      return switch (icebergType.typeId()) {
        case BOOLEAN -> GenericVortexReaders.bools();
        case INTEGER -> GenericVortexReaders.ints();
        case LONG -> longReader(primField.getType());
        case FLOAT -> GenericVortexReaders.floats();
        case DOUBLE -> doubleReader(primField.getType());
        case STRING -> SparkVortexValueReaders.utf8String(primField.getType());
        case BINARY -> SparkVortexValueReaders.bytes(primField.getType());
        case DECIMAL -> SparkVortexValueReaders.decimals();
        case TIMESTAMP, TIMESTAMP_NANO -> {
          ArrowType.Timestamp ts = (ArrowType.Timestamp) primField.getType();
          yield SparkVortexValueReaders.timestamp(ts.getUnit());
        }
        case TIME -> {
          ArrowType.Time time = (ArrowType.Time) primField.getType();
          yield SparkVortexValueReaders.time(time.getUnit());
        }
        case DATE -> SparkVortexValueReaders.date();
        case UUID -> SparkVortexValueReaders.uuid();
        default -> throw new UnsupportedOperationException("Unsupported type: " + icebergType);
      };
    }

    // Schema evolution: an int column may be projected as long after a type promotion.
    private static VortexValueReader<?> longReader(ArrowType arrowType) {
      return arrowType instanceof ArrowType.Int intType && intType.getBitWidth() <= Integer.SIZE
          ? GenericVortexReaders.intsAsLongs()
          : GenericVortexReaders.longs();
    }

    // Schema evolution: a float column may be projected as double after a type promotion.
    private static VortexValueReader<?> doubleReader(ArrowType arrowType) {
      return arrowType instanceof ArrowType.FloatingPoint fpType
              && fpType.getPrecision() == FloatingPointPrecision.SINGLE
          ? GenericVortexReaders.floatsAsDoubles()
          : GenericVortexReaders.doubles();
    }
  }

  static class StructReader implements VortexValueReader<InternalRow> {
    // File column name backing each expected field, or null when the field is absent from the file.
    private final String[] childNames;
    private final List<VortexValueReader<?>> fieldReaders;

    private StructReader(
        Types.StructType schema, List<Field> fields, List<VortexValueReader<?>> fieldReaders) {
      this.fieldReaders = Lists.newArrayListWithCapacity(fieldReaders.size());
      this.childNames = new String[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        this.childNames[i] = field == null ? null : field.getName();
        VortexValueReader<?> reader = fieldReaders.get(i);
        this.fieldReaders.add(reader != null ? reader : defaultReader(schema.fields().get(i)));
      }
    }

    @Override
    public InternalRow readNonNull(FieldVector vector, int row) {
      StructVector struct = (StructVector) vector;
      GenericInternalRow result = new GenericInternalRow(fieldReaders.size());
      for (int i = 0; i < fieldReaders.size(); i++) {
        VortexValueReader<?> fieldReader = fieldReaders.get(i);
        if (childNames[i] == null) {
          result.update(i, fieldReader.read(null, row));
        } else {
          FieldVector child = (FieldVector) struct.getChild(childNames[i]);
          result.update(i, fieldReader.read(child, row));
        }
      }
      return result;
    }
  }
}
