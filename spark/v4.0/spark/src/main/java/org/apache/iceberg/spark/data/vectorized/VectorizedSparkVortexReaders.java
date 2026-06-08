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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexBatchReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VectorizedSparkVortexReaders {
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
        Types.NestedField field = columns.get(i);
        int columnIndex = batchColumnIndex[i];
        if (columnIndex >= 0) {
          vectors[i] = new ArrowColumnVector(fieldVectors.get(columnIndex));
        } else if (idToConstant.containsKey(field.fieldId())) {
          vectors[i] =
              new ConstantColumnVector(field.type(), rowCount, idToConstant.get(field.fieldId()));
        } else if (field.fieldId() == MetadataColumns.IS_DELETED.fieldId()) {
          vectors[i] = new ConstantColumnVector(Types.BooleanType.get(), rowCount, false);
        } else {
          // Column is neither a constant nor present in the data file; surface nulls.
          vectors[i] = new ConstantColumnVector(field.type(), rowCount, null);
        }
      }

      return new ColumnarBatch(vectors, rowCount);
    }

    private int[] resolveColumns(List<FieldVector> fieldVectors) {
      Map<String, Integer> nameToIndex = Maps.newHashMapWithExpectedSize(fieldVectors.size());
      for (int i = 0; i < fieldVectors.size(); i++) {
        nameToIndex.put(fieldVectors.get(i).getField().getName(), i);
      }

      int[] indexes = new int[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        Types.NestedField field = columns.get(i);
        if (idToConstant.containsKey(field.fieldId())) {
          indexes[i] = -1;
        } else {
          Integer index = nameToIndex.get(field.name());
          indexes[i] = index == null ? -1 : index;
        }
      }

      return indexes;
    }
  }
}
