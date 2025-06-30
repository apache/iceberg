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

import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.ImmutableOrcBatchReadConf;
import org.apache.iceberg.spark.ImmutableParquetBatchReadConf;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.ParquetReaderType;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class BaseSparkPartitionReaderFactoryProvider
    implements SparkPartitionReaderFactoryProvider {

  public BaseSparkPartitionReaderFactoryProvider() {}

  @Override
  public PartitionReaderFactory createReaderFactory(SparkPartitionReaderFactoryConf conf) {
    return new Impl(conf.readConf(), conf.expectedSchema(), conf.taskGroups())
        .createReaderFactory();
  }

  private static class Impl {
    private final SparkReadConf readConf;
    private final Schema expectedSchema;
    private final List<? extends ScanTaskGroup<?>> taskGroups;

    private Impl(
        SparkReadConf readConf,
        Schema expectedSchema,
        List<? extends ScanTaskGroup<?>> taskGroups) {
      this.readConf = readConf;
      this.expectedSchema = expectedSchema;
      this.taskGroups = taskGroups;
    }

    public PartitionReaderFactory createReaderFactory() {
      if (useCometBatchReads()) {
        return new SparkColumnarReaderFactory(parquetBatchReadConf(ParquetReaderType.COMET));

      } else if (useParquetBatchReads()) {
        return new SparkColumnarReaderFactory(parquetBatchReadConf(ParquetReaderType.ICEBERG));

      } else if (useOrcBatchReads()) {
        return new SparkColumnarReaderFactory(orcBatchReadConf());

      } else {
        return new SparkRowReaderFactory();
      }
    }

    private ParquetBatchReadConf parquetBatchReadConf(ParquetReaderType readerType) {
      return ImmutableParquetBatchReadConf.builder()
          .batchSize(readConf.parquetBatchSize())
          .readerType(readerType)
          .build();
    }

    private OrcBatchReadConf orcBatchReadConf() {
      return ImmutableOrcBatchReadConf.builder().batchSize(readConf.parquetBatchSize()).build();
    }

    // conditions for using Parquet batch reads:
    // - Parquet vectorization is enabled
    // - only primitives or metadata columns are projected
    // - all tasks are of FileScanTask type and read only Parquet files
    private boolean useParquetBatchReads() {
      return readConf.parquetVectorizationEnabled()
          && expectedSchema.columns().stream().allMatch(this::supportsParquetBatchReads)
          && taskGroups.stream().allMatch(this::supportsParquetBatchReads);
    }

    private boolean supportsParquetBatchReads(ScanTask task) {
      if (task instanceof ScanTaskGroup) {
        ScanTaskGroup<?> taskGroup = (ScanTaskGroup<?>) task;
        return taskGroup.tasks().stream().allMatch(this::supportsParquetBatchReads);

      } else if (task.isFileScanTask() && !task.isDataTask()) {
        FileScanTask fileScanTask = task.asFileScanTask();
        return fileScanTask.file().format() == FileFormat.PARQUET;

      } else {
        return false;
      }
    }

    private boolean supportsParquetBatchReads(Types.NestedField field) {
      return field.type().isPrimitiveType() || MetadataColumns.isMetadataColumn(field.fieldId());
    }

    private boolean useCometBatchReads() {
      return readConf.parquetVectorizationEnabled()
          && readConf.parquetReaderType() == ParquetReaderType.COMET
          && expectedSchema.columns().stream().allMatch(this::supportsCometBatchReads)
          && taskGroups.stream().allMatch(this::supportsParquetBatchReads);
    }

    private boolean supportsCometBatchReads(Types.NestedField field) {
      return field.type().isPrimitiveType()
          && !field.type().typeId().equals(Type.TypeID.UUID)
          && field.fieldId() != MetadataColumns.ROW_ID.fieldId()
          && field.fieldId() != MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
    }

    // conditions for using ORC batch reads:
    // - ORC vectorization is enabled
    // - all tasks are of type FileScanTask and read only ORC files with no delete files
    private boolean useOrcBatchReads() {
      return readConf.orcVectorizationEnabled()
          && taskGroups.stream().allMatch(this::supportsOrcBatchReads);
    }

    private boolean supportsOrcBatchReads(ScanTask task) {
      if (task instanceof ScanTaskGroup) {
        ScanTaskGroup<?> taskGroup = (ScanTaskGroup<?>) task;
        return taskGroup.tasks().stream().allMatch(this::supportsOrcBatchReads);

      } else if (task.isFileScanTask() && !task.isDataTask()) {
        FileScanTask fileScanTask = task.asFileScanTask();
        return fileScanTask.file().format() == FileFormat.ORC && fileScanTask.deletes().isEmpty();

      } else {
        return false;
      }
    }
  }
}
