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
package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.ChangelogDataIterator;
import org.apache.iceberg.flink.source.RowDataChangelogScanTaskReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reader function that supports both normal data reading and CDC (changelog) reading. For normal
 * splits, it delegates to the standard RowDataReaderFunction. For changelog splits, it reads
 * ChangelogScanTasks with appropriate RowKind setting.
 */
public class ChangelogRowDataReaderFunction implements ReaderFunction<RowData> {
  private final Schema tableSchema;
  private final Schema readSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final List<Expression> filters;
  private final ReadableConfig config;

  // Delegate for normal (non-changelog) splits
  private final RowDataReaderFunction normalReaderFunction;

  public ChangelogRowDataReaderFunction(
      ReadableConfig config,
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters) {
    this(
        config,
        tableSchema,
        projectedSchema,
        nameMapping,
        caseSensitive,
        io,
        encryption,
        filters,
        -1L);
  }

  public ChangelogRowDataReaderFunction(
      ReadableConfig config,
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters,
      long limit) {
    this.config = config;
    this.tableSchema = tableSchema;
    this.readSchema = readSchema(tableSchema, projectedSchema);
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryption = encryption;
    this.filters = filters;
    this.normalReaderFunction =
        new RowDataReaderFunction(
            config,
            tableSchema,
            projectedSchema,
            nameMapping,
            caseSensitive,
            io,
            encryption,
            filters,
            limit);
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> apply(
      IcebergSourceSplit split) {
    if (split.isChangelogSplit()) {
      return createChangelogIterator(split);
    } else {
      return normalReaderFunction.apply(split);
    }
  }

  private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>>
      createChangelogIterator(IcebergSourceSplit split) {
    RowDataChangelogScanTaskReader taskReader =
        new RowDataChangelogScanTaskReader(
            tableSchema, readSchema, nameMapping, caseSensitive, filters, io, encryption);
    ChangelogDataIterator<RowData> changelogIterator =
        new ChangelogDataIterator<>(taskReader, split.changelogTasks());

    if (split.fileOffset() > 0 || split.recordOffset() > 0) {
      changelogIterator.seek(split.fileOffset(), split.recordOffset());
    }

    return new ChangelogBatchIterator(split.splitId(), changelogIterator, config);
  }

  private static Schema readSchema(Schema tableSchema, Schema projectedSchema) {
    Preconditions.checkNotNull(tableSchema, "Table schema can't be null");
    return projectedSchema == null ? tableSchema : projectedSchema;
  }

  /**
   * Batch iterator that wraps ChangelogDataIterator and produces ArrayBatchRecords for the Flink
   * source framework.
   */
  private class ChangelogBatchIterator
      implements CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> {

    private final String splitId;
    private final ChangelogDataIterator<RowData> inputIterator;
    private final RecordFactory<RowData> recordFactory;
    private final int batchSize;
    private final Pool<RowData[]> pool;

    ChangelogBatchIterator(
        String splitId, ChangelogDataIterator<RowData> inputIterator, ReadableConfig config) {
      this.splitId = splitId;
      this.inputIterator = inputIterator;
      this.recordFactory = new RowDataRecordFactory(FlinkSchemaUtil.convert(readSchema));
      this.batchSize = config.get(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT);
      int handoverQueueSize = config.get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
      this.pool = createPool(handoverQueueSize, batchSize);
    }

    private Pool<RowData[]> createPool(int numBatches, int batchCapacity) {
      Pool<RowData[]> batchPool = new Pool<>(numBatches);
      for (int i = 0; i < numBatches; i++) {
        batchPool.add(recordFactory.createBatch(batchCapacity));
      }
      return batchPool;
    }

    @Override
    public boolean hasNext() {
      return inputIterator.hasNext();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<RowData>> next() {
      if (!inputIterator.hasNext()) {
        throw new NoSuchElementException();
      }

      RowData[] batch = getCachedEntry();
      int recordCount = 0;
      while (inputIterator.hasNext() && recordCount < batchSize) {
        RowData nextRecord = inputIterator.next();
        recordFactory.clone(nextRecord, batch, recordCount);
        recordCount++;
        if (!inputIterator.currentTaskHasNext()) {
          break;
        }
      }

      return ArrayBatchRecords.forRecords(
          splitId,
          pool.recycler(),
          batch,
          recordCount,
          inputIterator.taskOffset(),
          inputIterator.recordOffset() - recordCount);
    }

    @Override
    public void close() throws IOException {
      inputIterator.close();
    }

    private RowData[] getCachedEntry() {
      try {
        return pool.pollEntry();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for array pool entry", e);
      }
    }
  }
}
