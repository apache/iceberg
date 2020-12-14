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
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.IcebergSourceOptions;
import org.apache.iceberg.flink.source.RowDataIterator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public class RowDataIteratorBulkFormat implements BulkFormat<RowData, IcebergSourceSplit> {

  private final TableInfo tableInfo;
  private final ScanContext scanContext;
  private final RowType rowType;
  private final TypeSerializer[] fieldSerializers;

  public RowDataIteratorBulkFormat(
      TableInfo tableInfo,
      ScanContext scanContext,
      RowType rowType) {
    this.tableInfo = tableInfo;
    this.scanContext = scanContext;
    this.rowType = rowType;
    this.fieldSerializers = rowType.getChildren().stream()
        .map(InternalSerializers::create)
        .toArray(TypeSerializer[]::new);
  }

  @Override
  public Reader<RowData> createReader(Configuration config, IcebergSourceSplit split) throws IOException {
    return createReaderInternal(config, split);
  }

  @Override
  public Reader<RowData> restoreReader(Configuration config, IcebergSourceSplit split) throws IOException {
    return createReaderInternal(config, split);
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return InternalTypeInfo.of(rowType);
  }

  private Reader<RowData> createReaderInternal(Configuration config, IcebergSourceSplit split) throws IOException {
    final DataIterator<RowData> inputIterator = new RowDataIterator(
        split.task(),
        tableInfo.fileIO(),
        tableInfo.encryptionManager(),
        tableInfo.schema(),
        scanContext.project(),
        scanContext.nameMapping(),
        scanContext.caseSensitive());
    if (split.checkpointedPosition() != null) {
      inputIterator.seek(split.checkpointedPosition());
    }
    return new RowDataIteratorReader(config, inputIterator);
  }

  private class RowDataIteratorReader implements BulkFormat.Reader<RowData> {

    private final DataIterator<RowData> inputIterator;
    private final int batchSize;
    private final Pool<RowData[]> pool;

    RowDataIteratorReader(Configuration config, DataIterator<RowData> inputIterator) {
      this.inputIterator = inputIterator;
      this.batchSize = config.getInteger(IcebergSourceOptions.READER_FETCH_BATCH_SIZE);
      this.pool = createPoolOfBatches(config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY));
    }

    @Nullable
    @Override
    public RecordIterator<RowData> readBatch() throws IOException {
      final RowData[] batch = getCachedEntry();
      int num = 0;
      while (inputIterator.hasNext() && num < batchSize) {
        RowData nextRecord = inputIterator.next();
        RowDataUtil.clone(nextRecord, batch[num], rowType, fieldSerializers);
        num++;
        if (inputIterator.isCurrentIteratorDone()) {
          // break early so that records in the ArrayResultIterator
          // have the same fileOffset.
          break;
        }
      }
      if (num == 0) {
        return null;
      } else {
        DataIterator.Position position = inputIterator.position();
        final RecyclableArrayIterator<RowData> outputIterator =
            new RecyclableArrayIterator<>(pool.recycler());
        outputIterator.set(batch, num, position.fileOffset(),
            position.recordOffset() - num);
        return outputIterator;
      }
    }

    @Override
    public void close() throws IOException {
      if (inputIterator != null) {
        inputIterator.close();
      }
    }

    private Pool<RowData[]> createPoolOfBatches(int numBatches) {
      final Pool<RowData[]> poolOfBatches = new Pool<>(numBatches);
      for (int batchId = 0; batchId < numBatches; batchId++) {
        RowData[] arr = new RowData[batchSize];
        for (int i = 0; i < batchSize; ++i) {
          arr[i] = new GenericRowData(rowType.getFieldCount());
        }
        poolOfBatches.add(arr);
      }
      return poolOfBatches;
    }

    private RowData[] getCachedEntry() throws IOException {
      try {
        return pool.pollEntry();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted");
      }
    }
  }
}
