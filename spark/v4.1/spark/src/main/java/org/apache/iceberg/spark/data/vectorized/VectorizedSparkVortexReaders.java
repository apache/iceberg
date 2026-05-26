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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
    return new SchemaCachingBatchReader(icebergSchema, vortexSchema, idToConstant);
  }

  static final class SchemaCachingBatchReader implements VortexBatchReader<ColumnarBatch> {
    private final Schema readerSchema;
    private final Map<Integer, ?> idToConstant;
    private final List<Integer> schemaMapping;

    SchemaCachingBatchReader(
        Schema readerSchema,
        org.apache.arrow.vector.types.pojo.Schema vortexSchema,
        Map<Integer, ?> idToConstant) {
      this.readerSchema = readerSchema;
      this.idToConstant = idToConstant;
      this.schemaMapping = vortexSchemaMapping(readerSchema, vortexSchema);
    }

    @Override
    public ColumnarBatch read(VectorSchemaRoot batch) {
      int rowCount = batch.getRowCount();
      Map<Integer, ColumnVector> vectors = Maps.newHashMap();

      for (Map.Entry<Integer, ?> entry : idToConstant.entrySet()) {
        Integer fieldId = entry.getKey();
        Object constant = entry.getValue();
        if (MetadataColumns.isMetadataColumn(fieldId)) {
          continue;
        }

        vectors.put(
            fieldId, new ConstantColumnVector(readerSchema.findType(fieldId), rowCount, constant));
      }

      List<FieldVector> fieldVectors = batch.getFieldVectors();
      for (int i = 0; i < fieldVectors.size(); i++) {
        int fieldId = schemaMapping.get(i);
        vectors.put(fieldId, new ArrowColumnVector(fieldVectors.get(i)));
      }

      return new ColumnarBatch(vectors.values().toArray(new ColumnVector[0]), rowCount);
    }

    // Mapping from Arrow Schema field index to Iceberg Field ID.
    static List<Integer> vortexSchemaMapping(
        Schema icebergSchema, org.apache.arrow.vector.types.pojo.Schema vortexSchema) {
      return vortexSchema.getFields().stream()
          .map(field -> icebergSchema.findField(field.getName()).fieldId())
          .collect(Collectors.toList());
    }
  }
}
