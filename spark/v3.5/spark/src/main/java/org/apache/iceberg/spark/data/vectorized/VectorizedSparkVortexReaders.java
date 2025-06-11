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

import dev.vortex.api.Array;
import dev.vortex.api.DType;
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.relocated.org.apache.arrow.vector.VectorSchemaRoot;
import dev.vortex.spark.read.VortexArrowColumnVector;
import dev.vortex.spark.read.VortexColumnarBatch;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.vortex.VortexBatchReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VectorizedSparkVortexReaders {
  private VectorizedSparkVortexReaders() {}

  public static VortexBatchReader<ColumnarBatch> buildReader(
      Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant) {
    return new SchemaCachingBatchReader(icebergSchema, vortexSchema, idToConstant);
  }

  static final class SchemaCachingBatchReader implements VortexBatchReader<ColumnarBatch> {
    private final Schema readerSchema;
    private final Map<Integer, ?> idToConstant;
    private final List<Integer> schemaMapping;

    // Reusable vector schema root.
    private VectorSchemaRoot root;

    SchemaCachingBatchReader(
        Schema readerSchema, DType vortexSchema, Map<Integer, ?> idToConstant) {
      this.readerSchema = readerSchema;
      this.idToConstant = idToConstant;
      this.schemaMapping = vortexSchemaMapping(readerSchema, vortexSchema);
    }

    @Override
    public ColumnarBatch read(Array batch) {
      this.root = batch.exportToArrow(ArrowAllocation.rootAllocator(), this.root);
      int rowCount = this.root.getRowCount();
      TreeMap<Integer, ColumnVector> vectors = new TreeMap<>();

      for (Map.Entry<Integer, ?> entry : idToConstant.entrySet()) {
        Integer fieldId = entry.getKey();
        Object constant = entry.getValue();
        if (MetadataColumns.isMetadataColumn(fieldId)) {
          continue;
        }

        // Field IDs are 1-indexed in Iceberg.
        vectors.put(
            fieldId,
            new ConstantColumnVector(
                readerSchema.findType(fieldId), (int) batch.getLen(), constant));
      }

      for (int i = 0; i < root.getFieldVectors().size(); i++) {
        int fieldId = schemaMapping.get(i);
        vectors.put(fieldId, new VortexArrowColumnVector(root.getVector(i)));
      }

      return new VortexColumnarBatch(
          batch, vectors.values().toArray(new ColumnVector[0]), rowCount);
    }

    // Mapping from Vortex schema index to Iceberg Field ID.
    static List<Integer> vortexSchemaMapping(Schema icebergSchema, DType vortexSchema) {
      // The schema mapping allows us to map the vortex schema into positions in the
      // return icebergSchema.
      List<String> fieldNames = vortexSchema.getFieldNames();

      // iceberg field id LUT
      Integer[] fieldIds =
          fieldNames.stream()
              .map(name -> icebergSchema.findField(name).fieldId())
              .toArray(Integer[]::new);

      // Return all of the integers
      IntStream.range(0, fieldNames.size())
          .mapToObj(i -> i)
          .sorted((i, j) -> fieldIds[i].compareTo(fieldIds[j]))
          .map(idx -> fieldNames);

      return vortexSchema.getFieldNames().stream()
          .map(fieldName -> icebergSchema.findField(fieldName).fieldId())
          .collect(Collectors.toList());
    }
  }
}
