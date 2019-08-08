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

package org.apache.iceberg.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

/***
 * Parquet Value Reader implementations for Vectorization.
 * Contains type-wise readers to read parquet data as vectors.
 * - Returns Arrow's Field Vector for each type.
 * - Null values are explicitly handled.
 * - Type serialization is done based on types in Arrow.
 * - Creates One Vector per RowGroup. So a Batch would have as many rows as there are in the underlying RowGroup.
 * - Mapping of Iceberg type to Arrow type is done in ArrowSchemaUtil.convert()
 * - Iceberg to Arrow Type mapping :
 *   icebergType : LONG       -   Field Vector Type : org.apache.arrow.vector.BigIntVector
 *   icebergType : STRING     -   Field Vector Type : org.apache.arrow.vector.VarCharVector
 *   icebergType : BOOLEAN    -   Field Vector Type : org.apache.arrow.vector.BitVector
 *   icebergType : INTEGER    -   Field Vector Type : org.apache.arrow.vector.IntVector
 *   icebergType : FLOAT      -   Field Vector Type : org.apache.arrow.vector.Float4Vector
 *   icebergType : DOUBLE     -   Field Vector Type : org.apache.arrow.vector.Float8Vector
 *   icebergType : DATE       -   Field Vector Type : org.apache.arrow.vector.DateDayVector
 *   icebergType : TIMESTAMP  -   Field Vector Type : org.apache.arrow.vector.TimeStampMicroTZVector
 *   icebergType : STRING     -   Field Vector Type : org.apache.arrow.vector.VarCharVector
 *   icebergType : BINARY     -   Field Vector Type : org.apache.arrow.vector.VarBinaryVector
 *   icebergField : DECIMAL   -   Field Vector Type : org.apache.arrow.vector.DecimalVector
 */
public class VectorReader implements BatchedReader {
  public static final int DEFAULT_NUM_ROWS_IN_BATCH = 10000;
  // private static final Logger LOG = LoggerFactory.getLogger(VectorReader.class);

  private final FieldVector vec;
  private int rowsInBatch = DEFAULT_NUM_ROWS_IN_BATCH;
  private final BatchedColumnIterator batchedColumnIterator;

  public VectorReader(ColumnDescriptor desc,
               Types.NestedField icebergField,
               BufferAllocator rootAlloc,
               int rowsInBatch) {
    this.rowsInBatch = (rowsInBatch == 0) ? DEFAULT_NUM_ROWS_IN_BATCH : rowsInBatch;
    this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
    this.batchedColumnIterator = new BatchedColumnIterator(desc, "", rowsInBatch); // what should be the writer version?
    // anjali
    // LOG.info("=> [VectorReader] rowsInBatch = " + this.rowsInBatch);
  }


  public FieldVector read() {
    vec.reset();
    if (batchedColumnIterator.hasNext()) {
      batchedColumnIterator.nextBatch(vec);
    }
    return vec;
  }

  public int getRowCount() {
    return vec.getValueCount();
  }

  public void setPageSource(PageReadStore source) {
    batchedColumnIterator.setPageSource(source);
  }
}

