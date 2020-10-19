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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;

abstract class BaseBatchScan implements Scan, Batch, SupportsReportStatistics {
  protected final Table table;
  protected final Schema expectedSchema;
  protected final Broadcast<FileIO> io;
  protected final Broadcast<EncryptionManager> encryptionManager;
  protected final boolean caseSensitive;
  protected final boolean localityPreferred;
  protected final Long splitSize;
  protected final Integer splitLookback;
  protected final Long splitOpenFileCost;
  protected final boolean batchReadsEnabled;
  protected final int batchSize;

  // lazy variables
  private StructType readSchema = null;

  BaseBatchScan(Table table, Schema expectedSchema, Broadcast<FileIO> io,
                Broadcast<EncryptionManager> encryptionManager, boolean caseSensitive,
                CaseInsensitiveStringMap options) {
    this.table = table;
    this.expectedSchema = expectedSchema;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.caseSensitive = caseSensitive;
    this.splitSize = Spark3Util.propertyAsLong(options, "split-size", null);
    this.splitLookback = Spark3Util.propertyAsInt(options, "lookback", null);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, "file-open-cost", null);
    this.localityPreferred = Spark3Util.isLocalityEnabled(io.value(), table.location(), options);
    this.batchReadsEnabled = Spark3Util.isVectorizationEnabled(table.properties(), options);
    this.batchSize = Spark3Util.batchSize(table.properties(), options);
  }

  protected abstract List<CombinedScanTask> tasks();

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);

    List<CombinedScanTask> scanTasks = tasks();
    InputPartition[] readTasks = new InputPartition[scanTasks.size()];
    for (int i = 0; i < scanTasks.size(); i++) {
      readTasks[i] = new ReadTask(
          scanTasks.get(i), tableSchemaString, expectedSchemaString, nameMappingString, io, encryptionManager,
          caseSensitive, localityPreferred);
    }

    return readTasks;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    boolean allParquetFileScanTasks =
        tasks().stream()
            .allMatch(combinedScanTask -> !combinedScanTask.isDataTask() && combinedScanTask.files()
                .stream()
                .allMatch(fileScanTask -> fileScanTask.file().format().equals(
                    FileFormat.PARQUET)));

    boolean allOrcFileScanTasks =
        tasks().stream()
            .allMatch(combinedScanTask -> !combinedScanTask.isDataTask() && combinedScanTask.files()
                .stream()
                .allMatch(fileScanTask -> fileScanTask.file().format().equals(
                    FileFormat.ORC)));

    boolean atLeastOneColumn = expectedSchema.columns().size() > 0;

    boolean onlyPrimitives = expectedSchema.columns().stream().allMatch(c -> c.type().isPrimitiveType());

    boolean hasNoDeleteFiles = tasks().stream().noneMatch(TableScanUtil::hasDeletes);

    boolean readUsingBatch = batchReadsEnabled && hasNoDeleteFiles && (allOrcFileScanTasks ||
        (allParquetFileScanTasks && atLeastOneColumn && onlyPrimitives));

    return new ReaderFactory(readUsingBatch ? batchSize : 0);
  }

  private static class ReaderFactory implements PartitionReaderFactory {
    private final int batchSize;

    ReaderFactory(int batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new RowReader((ReadTask) partition);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new BatchReader((ReadTask) partition, batchSize);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return batchSize > 1;
    }
  }

  private static class RowReader extends RowDataReader implements PartitionReader<InternalRow> {
    RowReader(ReadTask task) {
      super(task.task, task.tableSchema(), task.expectedSchema(), task.nameMappingString, task.io(), task.encryption(),
          task.isCaseSensitive());
    }
  }

  private static class BatchReader extends BatchDataReader implements PartitionReader<ColumnarBatch> {
    BatchReader(ReadTask task, int batchSize) {
      super(task.task, task.expectedSchema(), task.nameMappingString, task.io(), task.encryption(),
          task.isCaseSensitive(), batchSize);
    }
  }

  private static class ReadTask implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final String nameMappingString;
    private final Broadcast<FileIO> io;
    private final Broadcast<EncryptionManager> encryptionManager;
    private final boolean caseSensitive;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;
    private transient NameMapping nameMapping = null;
    private transient String[] preferredLocations = null;

    ReadTask(CombinedScanTask task, String tableSchemaString, String expectedSchemaString, String nameMappingString,
             Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager, boolean caseSensitive,
             boolean localityPreferred) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.nameMappingString = nameMappingString;
      this.io = io;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
      if (localityPreferred) {
        this.preferredLocations = Util.blockLocations(io.value(), task);
      } else {
        this.preferredLocations = HadoopInputFile.NO_LOCATION_PREFERENCE;
      }
    }

    @Override
    public String[] preferredLocations() {
      return preferredLocations;
    }

    public Collection<FileScanTask> files() {
      return task.files();
    }

    public FileIO io() {
      return io.value();
    }

    public EncryptionManager encryption() {
      return encryptionManager.value();
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    private Schema tableSchema() {
      if (tableSchema == null) {
        this.tableSchema = SchemaParser.fromJson(tableSchemaString);
      }
      return tableSchema;
    }

    private Schema expectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }

    private NameMapping nameMapping() {
      if (nameMapping == null) {
        this.nameMapping = NameMappingParser.fromJson(nameMappingString);
      }
      return nameMapping;
    }
  }
}
