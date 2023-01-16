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
package org.apache.iceberg.arrow.vectorized;

import java.util.List;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A collection of vectorized readers per column (in the expected read schema) and Arrow Vector
 * holders. This class owns the Arrow vectors and is responsible for closing the Arrow vectors.
 */
class ArrowBatchReader extends BaseBatchReader<ColumnarBatch> {

  ArrowBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
  }

  @Override
  public final ColumnarBatch read(ColumnarBatch reuse, int numRowsToRead) {
    Preconditions.checkArgument(
        numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);

    if (reuse == null) {
      closeVectors();
    }

    ColumnVector[] columnVectors = new ColumnVector[readers.length];
    for (int i = 0; i < readers.length; i += 1) {
      vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
      int numRowsInVector = vectorHolders[i].numValues();
      Preconditions.checkState(
          numRowsInVector == numRowsToRead,
          "Number of rows in the vector %s didn't match expected %s ",
          numRowsInVector,
          numRowsToRead);
      // Handle null vector for constant case
      columnVectors[i] = new ColumnVector(vectorHolders[i]);
    }
    return new ColumnarBatch(numRowsToRead, columnVectors);
  }
}
