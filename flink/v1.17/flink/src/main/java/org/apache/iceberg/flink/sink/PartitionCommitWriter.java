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
package org.apache.iceberg.flink.sink;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.PartitionedWriteResult;
import org.apache.iceberg.flink.util.PartitionCommitTriggerUtils;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/**
 * A {@link TaskWriter} for writing data to partitioned tables. Each partition has a writer, and
 * {@link DataFile}/{@link DeleteFile}/{@link CharSequenceSet} of each partition are stored
 * separately.
 */
public class PartitionCommitWriter implements TaskWriter<RowData> {
  private final Map<PartitionKey, PartitionedRollingFileWriter> writers = Maps.newHashMap();
  private final Set<PartitionKey> partitionKeys = Sets.newHashSet();
  private final Map<PartitionKey, List<DataFile>> completedDataFiles = Maps.newHashMap();
  private final Map<PartitionKey, List<DeleteFile>> completedDeleteFiles = Maps.newHashMap();
  private final Map<PartitionKey, CharSequenceSet> referencedDataFiles = Maps.newHashMap();

  private final FileFormat format;
  private final FileAppenderFactory<RowData> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private Throwable failure;

  private final PartitionKey partitionKey;
  private final RowDataWrapper wrapper;
  private final Duration commitDelay;
  private final String watermarkZoneId;
  private final String extractorPattern;
  private final String formatterPattern;
  private long watermark;

  protected PartitionCommitWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<RowData> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      Duration commitDelay,
      String watermarkZoneId,
      String extractorPattern,
      String formatterPattern) {
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;

    this.partitionKey = new PartitionKey(spec, schema);
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.commitDelay = commitDelay;
    this.watermarkZoneId = watermarkZoneId;
    this.extractorPattern = extractorPattern;
    this.formatterPattern = formatterPattern;
  }

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    Tasks.foreach(Iterables.concat(completedDataFiles.values(), completedDeleteFiles.values()))
        .executeWith(ThreadPools.getWorkerPool())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> file.forEach(f -> io.deleteFile(f.path().toString())));
  }

  @Override
  public WriteResult complete() throws IOException {
    return null;
  }

  @Override
  public void write(RowData row) throws IOException {
    partitionKey.partition(wrapper().wrap(row));

    PartitionedRollingFileWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in
      // writers.
      PartitionKey copiedKey = partitionKey.copy();
      partitionKeys.add(copiedKey);
      writer = new PartitionedRollingFileWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    writer.write(row);
  }

  public PartitionedWriteResult complete(long currentWatermark) throws IOException {
    this.watermark = currentWatermark;
    close();

    Iterator<PartitionKey> partitionKeyIterator = partitionKeys.iterator();

    while (partitionKeyIterator.hasNext()) {
      PartitionKey tempPartitionKey = partitionKeyIterator.next();
      if (PartitionCommitTriggerUtils.isPartitionCommittable(
          watermark,
          tempPartitionKey,
          commitDelay,
          watermarkZoneId,
          extractorPattern,
          formatterPattern)) {

        PartitionedWriteResult.PartitionWriteResultBuilder builder =
            PartitionedWriteResult.partitionWriteResultBuilder();

        List<DataFile> completedDataFile = completedDataFiles.remove(tempPartitionKey);
        if (completedDataFile != null) {
          builder.addDataFiles(completedDataFile);
        }

        List<DeleteFile> completedDeleteFile = completedDeleteFiles.remove(tempPartitionKey);
        if (completedDeleteFile != null) {
          builder.addDeleteFiles(completedDeleteFile);
        }

        CharSequenceSet referencedDataFile = referencedDataFiles.remove(tempPartitionKey);
        if (referencedDataFile != null) {
          builder.addReferencedDataFiles();
        }

        builder.partitionKey(tempPartitionKey);
        PartitionedWriteResult completePartitionResult = builder.build();

        partitionKeyIterator.remove();
        return completePartitionResult;
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    if (writers.isEmpty()) {
      return;
    }

    for (PartitionKey tempPartitionKey : partitionKeys) {
      if (PartitionCommitTriggerUtils.isPartitionCommittable(
          watermark,
          tempPartitionKey,
          commitDelay,
          watermarkZoneId,
          extractorPattern,
          formatterPattern)) {
        PartitionedRollingFileWriter writer = writers.get(tempPartitionKey);
        if (writer != null) {
          writer.close();
        }

        writers.remove(tempPartitionKey);
      }
    }
  }

  private class PartitionedRollingFileWriter implements Closeable {
    private static final int ROWS_DIVISOR = 1000;
    private final StructLike partitionKey;

    private EncryptedOutputFile currentFile = null;
    private DataWriter<RowData> currentWriter = null;
    private long currentRows = 0;

    private PartitionedRollingFileWriter(StructLike partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    private void complete(DataWriter<RowData> closedWriter) {
      completedDataFiles.compute(
          (PartitionKey) partitionKey,
          (newPartitionKey, dataFiles) -> {
            if (dataFiles == null) {
              return Lists.newArrayList(closedWriter.toDataFile());
            } else {
              dataFiles.add(closedWriter.toDataFile());
              return dataFiles;
            }
          });
    }

    public void write(RowData record) throws IOException {
      currentWriter.write(record);
      this.currentRows++;

      if (shouldRollToNewFile()) {
        closeCurrent();
        openCurrent();
      }
    }

    private void openCurrent() {
      if (partitionKey == null) {
        // unpartitioned
        this.currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        this.currentFile = fileFactory.newOutputFile(partitionKey);
      }
      this.currentWriter = appenderFactory.newDataWriter(currentFile, format, partitionKey);
      this.currentRows = 0;
    }

    private boolean shouldRollToNewFile() {
      return currentRows % ROWS_DIVISOR == 0 && currentWriter.length() >= targetFileSize;
    }

    private void closeCurrent() throws IOException {
      if (currentWriter != null) {
        try {
          currentWriter.close();

          if (currentRows == 0L) {
            try {
              io.deleteFile(currentFile.encryptingOutputFile());
            } catch (UncheckedIOException e) {
              // the file may not have been created, and it isn't worth failing the job to clean up,
              // skip deleting
            }
          } else {
            complete(currentWriter);
          }
        } catch (IOException | RuntimeException e) {
          setFailure(e);
          throw e;
        } finally {
          this.currentFile = null;
          this.currentWriter = null;
          this.currentRows = 0;
        }
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
    }
  }

  protected void setFailure(Throwable throwable) {
    if (failure == null) {
      this.failure = throwable;
    }
  }
}
