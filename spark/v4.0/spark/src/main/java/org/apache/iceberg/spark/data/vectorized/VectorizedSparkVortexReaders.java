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
package org.apache.iceberg.spark.data.vectorized;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexArrowProperties;
import org.apache.iceberg.vortex.VortexBatchReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class VectorizedSparkVortexReaders {

  static {
    // Configure Arrow's unsafe-memory-access and null-check-for-get properties like
    // VectorizedSparkParquetReaders does; Spark always calls isNullAt before value getters, so
    // Arrow's per-get null checks and bounds checks are redundant on this path.
    VortexArrowProperties.ensureConfigured();
  }

  private VectorizedSparkVortexReaders() {}

  public static VortexBatchReader<ColumnarBatch> buildReader(
      Schema icebergSchema,
      org.apache.arrow.vector.types.pojo.Schema vortexSchema,
      Map<Integer, ?> idToConstant) {
    return new ConstantAwareBatchReader(icebergSchema, idToConstant);
  }

  static final class ConstantAwareBatchReader implements VortexBatchReader<ColumnarBatch> {
    private final List<Types.NestedField> columns;
    private final Map<Integer, ?> idToConstant;

    // Resolves expected column position -> Arrow batch column index, computed by name from the
    // first batch. -1 marks a constant column not backed by a batch column. Vortex returns only the
    // projected (non-constant, file-resident) columns, so the batch is not positionally aligned
    // with
    // the reader schema.
    private int[] batchColumnIndex;

    ConstantAwareBatchReader(Schema readerSchema, Map<Integer, ?> idToConstant) {
      this.columns = readerSchema.columns();
      this.idToConstant = idToConstant == null ? Collections.emptyMap() : idToConstant;
    }

    @Override
    public ColumnarBatch read(VectorSchemaRoot batch) {
      int rowCount = batch.getRowCount();
      List<FieldVector> fieldVectors = batch.getFieldVectors();
      if (batchColumnIndex == null) {
        this.batchColumnIndex = resolveColumns(fieldVectors);
      }

      // Build columns in reader-schema order so they line up with Spark's expected output schema.
      ColumnVector[] vectors = new ColumnVector[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        vectors[i] = columnVector(columns.get(i), batchColumnIndex[i], fieldVectors, rowCount);
      }

      return new ColumnarBatch(vectors, rowCount);
    }

    private ColumnVector columnVector(
        Types.NestedField field, int columnIndex, List<FieldVector> fieldVectors, int rowCount) {
      int id = field.fieldId();
      if (columnIndex >= 0
          && id == MetadataColumns.ROW_ID.fieldId()
          && idToConstant.get(id) instanceof Long firstRowId) {
        // Row lineage: the scan packs {value: stored _row_id (when present), pos: row_idx}
        // under the _row_id name; stored values win and nulls inherit firstRowId + position.
        return new RowIdColumnVector(firstRowId, fieldVectors.get(columnIndex));
      } else if (columnIndex >= 0
          && id == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()
          && idToConstant.get(id) instanceof Long seqNumber) {
        // Stored values win; nulls inherit the file's sequence number.
        return new LongOrDefaultColumnVector(seqNumber, fieldVectors.get(columnIndex));
      } else if (columnIndex >= 0) {
        return new VortexArrowColumnVector(fieldVectors.get(columnIndex));
      } else if (idToConstant.containsKey(id)) {
        return new ConstantColumnVector(field.type(), rowCount, idToConstant.get(id));
      } else if (id == MetadataColumns.IS_DELETED.fieldId()) {
        return new ConstantColumnVector(Types.BooleanType.get(), rowCount, false);
      } else if (field.initialDefault() != null) {
        return new ConstantColumnVector(
            field.type(),
            rowCount,
            SparkUtil.internalToSpark(field.type(), field.initialDefault()));
      } else {
        // Column is neither a constant nor present in the data file; surface nulls.
        return new ConstantColumnVector(field.type(), rowCount, null);
      }
    }

    private int[] resolveColumns(List<FieldVector> fieldVectors) {
      Map<String, Integer> nameToIndex = Maps.newHashMapWithExpectedSize(fieldVectors.size());
      for (int i = 0; i < fieldVectors.size(); i++) {
        nameToIndex.put(fieldVectors.get(i).getField().getName(), i);
      }

      int[] indexes = new int[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        Types.NestedField field = columns.get(i);
        if (isRowLineageField(field) && idToConstant.get(field.fieldId()) instanceof Long) {
          // Row lineage columns are materialized by the scan when the engine supplies an
          // inheritance base; bind them by name (absent columns fall back to the constant).
          Integer index = nameToIndex.get(field.name());
          indexes[i] = index == null ? -1 : index;
        } else if (idToConstant.containsKey(field.fieldId())) {
          indexes[i] = -1;
        } else {
          Integer index = nameToIndex.get(field.name());
          indexes[i] = index == null ? -1 : index;
        }
      }

      return indexes;
    }

    private static boolean isRowLineageField(Types.NestedField field) {
      return field.fieldId() == MetadataColumns.ROW_ID.fieldId()
          || field.fieldId() == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
    }
  }

  /**
   * A long {@link ColumnVector} for the {@code _row_id} metadata column, bound to a struct packed
   * by the scan as {@code {value: <stored _row_id, when the file has one>, pos: row_idx}}. Stored
   * values win; null (or absent) values inherit {@code firstRowId + position}, per the row lineage
   * spec.
   */
  private static final class RowIdColumnVector extends ColumnVector {
    private final long firstRowId;
    private final StructVector vector;
    private final FieldVector value;
    private final FieldVector pos;

    private RowIdColumnVector(long firstRowId, FieldVector vector) {
      super(DataTypes.LongType);
      this.firstRowId = firstRowId;
      this.vector = (StructVector) vector;
      this.value = (FieldVector) this.vector.getChild("value");
      this.pos = (FieldVector) this.vector.getChild("pos");
    }

    @Override
    public long getLong(int rowId) {
      if (value != null && !value.isNull(rowId)) {
        return ((BaseIntVector) value).getValueAsLong(rowId);
      }

      return firstRowId + ((BaseIntVector) pos).getValueAsLong(rowId);
    }

    @Override
    public boolean isNullAt(int rowId) {
      return false;
    }

    @Override
    public boolean hasNull() {
      return false;
    }

    @Override
    public int numNulls() {
      return 0;
    }

    @Override
    public void close() {
      vector.close();
    }

    @Override
    public boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public byte getByte(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public short getShort(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public int getInt(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public float getFloat(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public double getDouble(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }
  }

  /**
   * A long {@link ColumnVector} that substitutes a constant when the stored value is null, used for
   * the {@code _last_updated_sequence_number} metadata column.
   */
  private static final class LongOrDefaultColumnVector extends ColumnVector {
    private final long defaultValue;
    private final FieldVector vector;

    private LongOrDefaultColumnVector(long defaultValue, FieldVector vector) {
      super(DataTypes.LongType);
      this.defaultValue = defaultValue;
      this.vector = vector;
    }

    @Override
    public long getLong(int rowId) {
      if (vector.isNull(rowId)) {
        return defaultValue;
      }

      return ((BaseIntVector) vector).getValueAsLong(rowId);
    }

    @Override
    public boolean isNullAt(int rowId) {
      return false;
    }

    @Override
    public boolean hasNull() {
      return false;
    }

    @Override
    public int numNulls() {
      return 0;
    }

    @Override
    public void close() {
      vector.close();
    }

    @Override
    public boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public byte getByte(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public short getShort(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public int getInt(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public float getFloat(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public double getDouble(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      throw new UnsupportedOperationException("Row lineage columns are longs");
    }
  }
}
