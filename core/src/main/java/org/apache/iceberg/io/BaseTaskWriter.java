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

package org.apache.iceberg.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.Tasks;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  private final List<DataFile> completedDataFiles = Lists.newArrayList();
  private final List<DeleteFile> completedDeleteFiles = Lists.newArrayList();
  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;

  protected BaseTaskWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                           OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    Tasks.foreach(Iterables.concat(completedDataFiles, completedDeleteFiles))
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public WriteResult complete() throws IOException {
    close();

    return WriteResult.builder()
        .addDataFiles(completedDataFiles)
        .addDeleteFiles(completedDeleteFiles)
        .build();
  }

  /**
   * Base delta writer to write both insert records and equality-deletes.
   */
  protected abstract class BaseDeltaWriter implements Closeable {
    private final RollingFileWriter dataWriter;
    private final RollingEqDeleteWriter eqDeleteWriter;
    private final SortedPosDeleteWriter<T> posDeleteWriter;
    private final StructLikeMap<PathOffset> insertedRowMap;

    public BaseDeltaWriter(PartitionKey partition, Schema eqDeleteSchema) {
      Preconditions.checkNotNull(eqDeleteSchema, "equality-delete schema could not be null.");

      this.dataWriter = new RollingFileWriter(partition);

      this.eqDeleteWriter = new RollingEqDeleteWriter(partition);
      this.insertedRowMap = StructLikeMap.create(eqDeleteSchema.asStruct());

      this.posDeleteWriter = new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, partition);
    }

    protected abstract StructLike asStructLike(T row);

    protected abstract StructLike asCopiedStructLike(T row);

    public void write(T row) throws IOException {
      PathOffset pathOffset = PathOffset.of(dataWriter.currentPath(), dataWriter.currentRows());

      StructLike copiedKey = asCopiedStructLike(row);
      // Adding a pos-delete to replace the old filePos.
      PathOffset previous = insertedRowMap.put(copiedKey, pathOffset);
      if (previous != null) {
        // TODO attach the previous row if has a positional-delete row schema in appender factory.
        posDeleteWriter.delete(previous.path, previous.rowOffset, null);
      }

      dataWriter.write(row);
    }

    public void delete(T row) throws IOException {
      StructLike key = asStructLike(row);
      PathOffset previous = insertedRowMap.remove(key);

      if (previous != null) {
        // TODO attach the previous row if has a positional-delete row schema in appender factory.
        posDeleteWriter.delete(previous.path, previous.rowOffset, null);
      }

      eqDeleteWriter.write(row);
    }

    @Override
    public void close() throws IOException {
      // Close data writer and add completed data files.
      dataWriter.close();

      // Close eq-delete writer and add completed equality-delete files.
      eqDeleteWriter.close();
      insertedRowMap.clear();

      // add the completed pos-delete files.
      completedDeleteFiles.addAll(posDeleteWriter.complete());
    }
  }

  private static class PathOffset {
    private final CharSequence path;
    private final long rowOffset;

    private PathOffset(CharSequence path, long rowOffset) {
      this.path = path;
      this.rowOffset = rowOffset;
    }

    private static PathOffset of(CharSequence path, long rowOffset) {
      return new PathOffset(path, rowOffset);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("path", path)
          .add("row_offset", rowOffset)
          .toString();
    }
  }

  private abstract class BaseRollingWriter<W extends Closeable> implements Closeable {
    private static final int ROWS_DIVISOR = 1000;
    private final PartitionKey partitionKey;

    private EncryptedOutputFile currentFile = null;
    private W currentWriter = null;
    private long currentRows = 0;

    private BaseRollingWriter(PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    abstract W newWriter(EncryptedOutputFile file, PartitionKey key);

    abstract long length(W writer);

    abstract void write(W writer, T record);

    abstract void complete(W closedWriter);

    public void write(T record) throws IOException {
      write(currentWriter, record);
      this.currentRows++;

      if (shouldRollToNewFile()) {
        closeCurrent();
        openCurrent();
      }
    }

    public CharSequence currentPath() {
      Preconditions.checkNotNull(currentFile, "The currentFile shouldn't be null");
      return currentFile.encryptingOutputFile().location();
    }

    public long currentRows() {
      return currentRows;
    }

    private void openCurrent() {
      if (partitionKey == null) {
        // unpartitioned
        this.currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        this.currentFile = fileFactory.newOutputFile(partitionKey);
      }
      this.currentWriter = newWriter(currentFile, partitionKey);
      this.currentRows = 0;
    }

    private boolean shouldRollToNewFile() {
      // TODO: ORC file now not support target file size before closed
      return !format.equals(FileFormat.ORC) &&
          currentRows % ROWS_DIVISOR == 0 && length(currentWriter) >= targetFileSize;
    }

    private void closeCurrent() throws IOException {
      if (currentWriter != null) {
        currentWriter.close();

        if (currentRows == 0L) {
          io.deleteFile(currentFile.encryptingOutputFile());
        } else {
          complete(currentWriter);
        }

        this.currentFile = null;
        this.currentWriter = null;
        this.currentRows = 0;
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
    }
  }

  protected class RollingFileWriter extends BaseRollingWriter<DataWriter<T>> {
    public RollingFileWriter(PartitionKey partitionKey) {
      super(partitionKey);
    }

    @Override
    DataWriter<T> newWriter(EncryptedOutputFile file, PartitionKey key) {
      return appenderFactory.newDataWriter(file, format, key);
    }

    @Override
    long length(DataWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(DataWriter<T> writer, T record) {
      writer.add(record);
    }

    @Override
    void complete(DataWriter<T> closedWriter) {
      completedDataFiles.add(closedWriter.toDataFile());
    }
  }

  protected class RollingEqDeleteWriter extends BaseRollingWriter<EqualityDeleteWriter<T>> {
    RollingEqDeleteWriter(PartitionKey partitionKey) {
      super(partitionKey);
    }

    @Override
    EqualityDeleteWriter<T> newWriter(EncryptedOutputFile file, PartitionKey key) {
      return appenderFactory.newEqDeleteWriter(file, format, key);
    }

    @Override
    long length(EqualityDeleteWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(EqualityDeleteWriter<T> writer, T record) {
      writer.delete(record);
    }

    @Override
    void complete(EqualityDeleteWriter<T> closedWriter) {
      completedDeleteFiles.add(closedWriter.toDeleteFile());
    }
  }
}
