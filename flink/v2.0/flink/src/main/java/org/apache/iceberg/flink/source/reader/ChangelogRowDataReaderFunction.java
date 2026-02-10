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

import java.util.List;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.ChangelogDataIterator;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataChangelogScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reader function that supports both normal data reading and CDC (changelog) reading. This function
 * detects whether a split contains changelog tasks and uses the appropriate reader.
 */
public class ChangelogRowDataReaderFunction extends DataIteratorReaderFunction<RowData> {
  private final Schema tableSchema;
  private final Schema readSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final List<Expression> filters;
  private final long limit;

  private transient RecordLimiter recordLimiter = null;

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
    super(
        new ArrayPoolDataIteratorBatcher<>(
            config,
            new RowDataRecordFactory(
                FlinkSchemaUtil.convert(readSchema(tableSchema, projectedSchema)))));
    this.tableSchema = tableSchema;
    this.readSchema = readSchema(tableSchema, projectedSchema);
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryption = encryption;
    this.filters = filters;
    this.limit = limit;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> apply(
      IcebergSourceSplit split) {
    if (split.isChangelogSplit()) {
      // Use changelog data iterator for CDC splits
      ChangelogDataIterator<RowData> changelogIterator = createChangelogDataIterator(split);

      // Seek to the saved position if needed
      if (split.fileOffset() > 0 || split.recordOffset() > 0) {
        changelogIterator.seek(split.fileOffset(), split.recordOffset());
      }

      return wrapIterator(changelogIterator, split);
    } else {
      // Use normal data iterator for regular splits
      return super.apply(split);
    }
  }

  @Override
  public DataIterator<RowData> createDataIterator(IcebergSourceSplit split) {
    return new LimitableDataIterator<>(
        new RowDataFileScanTaskReader(tableSchema, readSchema, nameMapping, caseSensitive, filters),
        split.task(),
        io,
        encryption,
        lazyLimiter());
  }

  private ChangelogDataIterator<RowData> createChangelogDataIterator(IcebergSourceSplit split) {
    RowDataChangelogScanTaskReader taskReader =
        new RowDataChangelogScanTaskReader(
            tableSchema, readSchema, nameMapping, caseSensitive, filters, io, encryption);
    return new ChangelogDataIterator<>(taskReader, split.changelogTasks());
  }

  private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> wrapIterator(
      ChangelogDataIterator<RowData> changelogIterator, IcebergSourceSplit split) {
    return new ChangelogBatchingIterator(changelogIterator, split, batcher(), lazyLimiter());
  }

  private static Schema readSchema(Schema tableSchema, Schema projectedSchema) {
    Preconditions.checkNotNull(tableSchema, "Table schema can't be null");
    return projectedSchema == null ? tableSchema : projectedSchema;
  }

  /** Lazily create RecordLimiter to avoid the need to make it serializable */
  private RecordLimiter lazyLimiter() {
    if (recordLimiter == null) {
      this.recordLimiter = RecordLimiter.create(limit);
    }

    return recordLimiter;
  }

  /**
   * Iterator that wraps ChangelogDataIterator and batches records for the Flink source framework.
   */
  private static class ChangelogBatchingIterator
      implements CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> {

    private final ChangelogDataIterator<RowData> changelogIterator;
    private final IcebergSourceSplit split;
    private final DataIteratorBatcher<RowData> batcher;
    private final RecordLimiter limiter;
    private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> currentBatch;
    private boolean finished;

    ChangelogBatchingIterator(
        ChangelogDataIterator<RowData> changelogIterator,
        IcebergSourceSplit split,
        DataIteratorBatcher<RowData> batcher,
        RecordLimiter limiter) {
      this.changelogIterator = changelogIterator;
      this.split = split;
      this.batcher = batcher;
      this.limiter = limiter;
      this.finished = false;
    }

    @Override
    public boolean hasNext() {
      if (finished) {
        return false;
      }

      if (currentBatch != null && currentBatch.hasNext()) {
        return true;
      }

      // Create a new batch from changelog records
      if (changelogIterator.hasNext() && !limiter.reachedLimit()) {
        currentBatch = batcher.batch(split.splitId(), createLimitedIterator());
        return currentBatch.hasNext();
      }

      finished = true;
      return false;
    }

    private CloseableIterator<RowData> createLimitedIterator() {
      return new CloseableIterator<RowData>() {
        @Override
        public boolean hasNext() {
          return changelogIterator.hasNext() && !limiter.reachedLimit();
        }

        @Override
        public RowData next() {
          limiter.increment();
          return changelogIterator.next();
        }

        @Override
        public void close() {
          // Don't close the underlying iterator here
        }
      };
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<RowData>> next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }

      // Update split position
      split.updatePosition(changelogIterator.taskOffset(), changelogIterator.recordOffset());

      return currentBatch.next();
    }

    @Override
    public void close() throws java.io.IOException {
      changelogIterator.close();
      if (currentBatch != null) {
        currentBatch.close();
      }
    }
  }
}
