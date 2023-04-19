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
package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.io.CloseableIterator;

/**
 * Iterator wrapper around Hive's VectorizedRowBatch producer (MRv1 implementing) record readers.
 */
public final class VectorizedRowBatchIterator implements CloseableIterator<VectorizedRowBatch> {

  private final RecordReader<NullWritable, VectorizedRowBatch> recordReader;
  private final NullWritable key;
  private final VectorizedRowBatch batch;
  private final VectorizedRowBatchCtx vrbCtx;
  private final int[] partitionColIndices;
  private final Object[] partitionValues;
  private boolean advanced = false;

  VectorizedRowBatchIterator(
      RecordReader<NullWritable, VectorizedRowBatch> recordReader,
      JobConf job,
      int[] partitionColIndices,
      Object[] partitionValues) {
    this.recordReader = recordReader;
    this.key = recordReader.createKey();
    this.batch = recordReader.createValue();
    this.vrbCtx = CompatibilityHiveVectorUtils.findMapWork(job).getVectorizedRowBatchCtx();
    this.partitionColIndices = partitionColIndices;
    this.partitionValues = partitionValues;
  }

  @Override
  public void close() throws IOException {
    this.recordReader.close();
  }

  private void advance() {
    if (!advanced) {
      try {

        if (!recordReader.next(key, batch)) {
          batch.size = 0;
        }
        // Fill partition values
        if (partitionColIndices != null) {
          for (int i = 0; i < partitionColIndices.length; ++i) {
            int colIdx = partitionColIndices[i];
            CompatibilityHiveVectorUtils.addPartitionColsToBatch(
                batch.cols[colIdx],
                partitionValues[i],
                vrbCtx.getRowColumnNames()[colIdx],
                vrbCtx.getRowColumnTypeInfos()[colIdx]);
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      advanced = true;
    }
  }

  @Override
  public boolean hasNext() {
    advance();
    return batch.size > 0;
  }

  @Override
  public VectorizedRowBatch next() {
    advance();
    advanced = false;
    return batch;
  }
}
