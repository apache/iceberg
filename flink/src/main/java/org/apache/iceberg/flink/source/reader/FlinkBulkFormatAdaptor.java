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
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This adaptor use Flink BulkFormat implementation to read data file.
 * Note that Flink BulkFormat may not support row deletes like {@link RowDataIteratorBulkFormat}
 */
public class FlinkBulkFormatAdaptor<T> implements BulkFormat<T, IcebergSourceSplit> {

  private final Map<FileFormat, BulkFormat<T, FileSourceSplit>> bulkFormatProvider;
  private final TypeInformation<T> producedType;

  public FlinkBulkFormatAdaptor(Map<FileFormat, BulkFormat<T, FileSourceSplit>> bulkFormatProvider) {
    this.bulkFormatProvider = bulkFormatProvider;
    // validate that all BulkFormat produce the same type
    List<TypeInformation<T>> uniqueTypes = bulkFormatProvider.values().stream()
        .map(bulkFormat -> bulkFormat.getProducedType())
        .distinct()
        .collect(Collectors.toList());
    Preconditions.checkArgument(uniqueTypes.size() == 1,
        "BulkFormats have the different producedType: " + uniqueTypes);
    producedType = uniqueTypes.get(0);
  }

  @Override
  public Reader<T> createReader(Configuration config, IcebergSourceSplit split) throws IOException {
    return new ReaderAdaptor<T>(bulkFormatProvider, config, split, false);
  }

  @Override
  public Reader<T> restoreReader(Configuration config, IcebergSourceSplit split) throws IOException {
    return new ReaderAdaptor<T>(bulkFormatProvider, config, split, true);
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return producedType;
  }

  private static final class ReaderAdaptor<T> implements BulkFormat.Reader<T> {

    private final Map<FileFormat, BulkFormat<T, FileSourceSplit>> bulkFormatProvider;
    private final Configuration config;
    private final Iterator<FileScanTask> fileIterator;
    private final boolean isRestored;

    // file offset in CombinedScanTask
    private int fileOffset = -1;
    private Reader<T> currentReader;

    ReaderAdaptor(
        Map<FileFormat, BulkFormat<T, FileSourceSplit>> bulkFormatProvider,
        Configuration config,
        IcebergSourceSplit icebergSplit,
        boolean isRestored) throws IOException {
      this.config = config;
      this.bulkFormatProvider = bulkFormatProvider;
      this.fileIterator = icebergSplit.task().files().iterator();
      this.isRestored = isRestored;

      final CheckpointedPosition position = icebergSplit.checkpointedPosition();
      if (position != null) {
        // skip files based on offset in checkpointed position
        Preconditions.checkArgument(position.getOffset() < icebergSplit.task().files().size(),
            String.format("Checkpointed file offset is %d, while CombinedScanTask has %d files",
                position.getOffset(), icebergSplit.task().files().size()));
        for (int i = 0; i < position.getOffset(); ++i) {
          fileIterator.next();
          fileOffset++;
        }
        // first file may need to skip records
        setupReader(position.getRecordsAfterOffset());
      } else {
        setupReader(0L);
      }
    }

    /**
     * TODO: we can't return RecordIterator with empty data.
     * Otherwise, caller may assume it is end of input.
     * We probably need to add a {@code hasNext()} API to
     * {@link RecordIterator} to achieve the goal.
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
      RecordIterator<T> result = null;
      while (currentReader != null || fileIterator.hasNext()) {
        if (currentReader == null) {
          setupReader(0L);
        } else {
          result = currentReader.readBatch();
          if (result != null) {
            break;
          } else {
            closeCurrentReader();
          }
        }
      }
      if (result == null) {
        return null;
      } else {
        return new RecordIteratorAdaptor(fileOffset, result);
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrentReader();
    }

    private void closeCurrentReader() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
    }

    private void setupReader(long skipRecordCount) throws IOException {
      if (fileIterator.hasNext()) {
        final FileScanTask fileScanTask = fileIterator.next();
        final FileFormat fileFormat = fileScanTask.file().format();
        if (!bulkFormatProvider.containsKey(fileFormat)) {
          throw new IOException("Unsupported file format: " + fileFormat);
        }
        final BulkFormat<T, FileSourceSplit> bulkFormat = bulkFormatProvider.get(fileFormat);
        fileOffset++;
        final FileSourceSplit fileSourceSplit = new FileSourceSplit(
            "",
            new Path(URI.create(fileScanTask.file().path().toString())),
            fileScanTask.start(),
            fileScanTask.length(),
            new String[0],
            // Since this is always for a single data file and some format
            // (like ParquetVectorizedInputFormat) requires NO_OFFSET,
            // we just always set the file offset to NO_OFFSET.
            new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, skipRecordCount));
        if (isRestored) {
          currentReader = bulkFormat.restoreReader(config, fileSourceSplit);
        } else {
          currentReader = bulkFormat.createReader(config, fileSourceSplit);
        }
      } else {
        closeCurrentReader();
      }
    }
  }

  private static final class RecordIteratorAdaptor<T> implements RecordIterator<T> {

    private final long fileOffset;
    private final RecordIterator<T> iterator;
    private final MutableRecordAndPosition mutableRecordAndPosition;

    RecordIteratorAdaptor(long fileOffset, RecordIterator<T> iterator) {
      this.fileOffset = fileOffset;
      this.iterator = iterator;
      this.mutableRecordAndPosition = new MutableRecordAndPosition();
    }

    @Nullable
    @Override
    public RecordAndPosition<T> next() {
      RecordAndPosition<T> original = iterator.next();
      if (original != null) {
        mutableRecordAndPosition.set(original.getRecord(), fileOffset, original.getRecordSkipCount());
        return mutableRecordAndPosition;
      } else {
        return null;
      }
    }

    @Override
    public void releaseBatch() {
      iterator.releaseBatch();
    }
  }

}
