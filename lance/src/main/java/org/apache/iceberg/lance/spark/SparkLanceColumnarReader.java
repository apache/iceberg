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
package org.apache.iceberg.lance.spark;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Converts Arrow VectorSchemaRoot batches from Lance into Spark ColumnarBatch.
 *
 * <p>This is a near-zero-copy path: Arrow FieldVectors are wrapped directly in ArrowColumnVector
 * without data copying.
 */
public class SparkLanceColumnarReader {

  private SparkLanceColumnarReader() {}

  /**
   * Builds a reader function that converts Arrow batches to ColumnarBatch.
   *
   * @param expectedSchema the Iceberg schema defining expected columns
   * @param idToConstant constant values for partition/metadata columns
   * @return a function that converts (VectorSchemaRoot, idToConstant) → CloseableIterable
   */
  public static Function<
          Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<ColumnarBatch>>
      buildReader(Schema expectedSchema, Map<Integer, ?> idToConstant) {
    return entry -> {
      VectorSchemaRoot batch = entry.getKey();
      int rowCount = batch.getRowCount();

      List<ColumnVector> columns = Lists.newArrayList();
      for (Types.NestedField field : expectedSchema.columns()) {
        DataType sparkType = IcebergTypeToSparkType.convert(field.type());
        if (idToConstant != null && idToConstant.containsKey(field.fieldId())) {
          columns.add(
              new ConstantColumnVector(sparkType, rowCount, idToConstant.get(field.fieldId())));
        } else {
          FieldVector arrowVector = batch.getVector(field.name());
          if (arrowVector != null) {
            columns.add(new ArrowColumnVector(arrowVector));
          } else {
            columns.add(new ConstantColumnVector(sparkType, rowCount, null));
          }
        }
      }

      ColumnarBatch columnarBatch = new ColumnarBatch(columns.toArray(new ColumnVector[0]));
      columnarBatch.setNumRows(rowCount);

      return new CloseableIterable<ColumnarBatch>() {
        @Override
        public CloseableIterator<ColumnarBatch> iterator() {
          return CloseableIterator.withClose(Collections.singletonList(columnarBatch).iterator());
        }

        @Override
        public void close() throws IOException {
          // batch lifecycle managed by the LanceCloseableIterator
        }
      };
    };
  }
}
