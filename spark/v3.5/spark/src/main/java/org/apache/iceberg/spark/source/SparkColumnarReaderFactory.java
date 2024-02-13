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
package org.apache.iceberg.spark.source;

import java.util.Properties;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class SparkColumnarReaderFactory implements PartitionReaderFactory {
  private final int batchSize;
  private final String customizedVectorizationImpl;

  private final Properties customizedVectorizationProperties;

  SparkColumnarReaderFactory(
      int batchSize, String vectorizationImpl, Properties customizedVectorizationProperties) {
    Preconditions.checkArgument(batchSize > 1, "Batch size must be > 1");
    this.batchSize = batchSize;
    this.customizedVectorizationImpl = vectorizationImpl;
    this.customizedVectorizationProperties = customizedVectorizationProperties;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
    throw new UnsupportedOperationException("Row-based reads are not supported");
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition inputPartition) {
    Preconditions.checkArgument(
        inputPartition instanceof SparkInputPartition,
        "Unknown input partition type: %s",
        inputPartition.getClass().getName());

    SparkInputPartition partition = (SparkInputPartition) inputPartition;

    if (partition.allTasksOfType(FileScanTask.class)) {
      BatchDataReader batchDataReader = new BatchDataReader(partition, batchSize);
      batchDataReader.setCustomizedVectorizationImpl(customizedVectorizationImpl);
      batchDataReader.setCustomizedVectorizationProperties(customizedVectorizationProperties);
      return batchDataReader;

    } else {
      throw new UnsupportedOperationException(
          "Unsupported task group for columnar reads: " + partition.taskGroup());
    }
  }

  @Override
  public boolean supportColumnarReads(InputPartition inputPartition) {
    return true;
  }
}
