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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  private final List<DataFile> completedDataFiles = Lists.newArrayList();
  private final List<DeleteFile> completedDeleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private Throwable failure;

  protected BaseTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  protected PartitionSpec spec() {
    return spec;
  }

  protected void setFailure(Throwable throwable) {
    if (failure == null) {
      this.failure = throwable;
    }
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    Tasks.foreach(Iterables.concat(completedDataFiles, completedDeleteFiles))
        .executeWith(ThreadPools.getWorkerPool())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public WriteResult complete() throws IOException {
    close();

    Preconditions.checkState(failure == null, "Cannot return results from failed writer", failure);

    return WriteResult.builder()
        .addDataFiles(completedDataFiles)
        .addDeleteFiles(completedDeleteFiles)
        .addReferencedDataFiles(referencedDataFiles)
        .build();
  }

  /** Base equality delta writer to write both insert records and equality-deletes. */
  protected abstract class BaseEqualityDeltaWriter implements Closeable {
    private final StructProjection structProjection;
    private final PositionDelete<T> positionDelete;
    private RollingFileWriter dataWriter;
    private RollingEqDeleteWriter eqDeleteWriter;
    private FileWriter<PositionDelete<T>, DeleteWriteResult> posDeleteWriter;
    private Map<StructLike, PathOffset> insertedRowMap;

    protected BaseEqualityDeltaWriter(StructLike partition, Schema schema, Schema deleteSchema) {
      this(partition, schema, deleteSchema, DeleteGranularity.PARTITION);
    }

    protected BaseEqualityDeltaWriter(
        StructLike partition,
        Schema schema,
        Schema deleteSchema,
        DeleteGranularity deleteGranularity) {
      Preconditions.checkNotNull(schema, "Iceberg table schema cannot be null.");
      Preconditions.checkNotNull(deleteSchema, "Equality-delete schema cannot be null.");
      this.structProjection = StructProjection.create(schema, deleteSchema);
      this.positionDelete = PositionDelete.create();

      this.dataWriter = new RollingFileWriter(partition);
      this.eqDeleteWriter = new RollingEqDeleteWriter(partition);
      this.posDeleteWriter =
          new SortingPositionOnlyDeleteWriter<>(
              () -> appenderFactory.newPosDeleteWriter(newOutputFile(partition), format, partition),
              deleteGranularity);
      this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
    }

    /** Wrap the data as a {@link StructLike}. */
    protected abstract StructLike asStructLike(T data);

    /** Wrap the passed in key of a row as a {@link StructLike} */
    protected abstract StructLike asStructLikeKey(T key);

    public void write(T row) throws IOException {
      PathOffset pathOffset = PathOffset.of(dataWriter.currentPath(), dataWriter.currentRows());

      // Create a copied key from this row.
      StructLike copiedKey = StructCopy.copy(structProjection.wrap(asStructLike(row)));

      // Adding a pos-delete to replace the old path-offset.
      PathOffset previous = insertedRowMap.put(copiedKey, pathOffset);
      if (previous != null) {
        // TODO attach the previous row if has a positional-delete row schema in appender factory.
        writePosDelete(previous);
      }

      dataWriter.write(row);
    }

    private EncryptedOutputFile newOutputFile(StructLike partition) {
      if (spec.isUnpartitioned() || partition == null) {
        return fileFactory.newOutputFile();
      } else {
        return fileFactory.newOutputFile(spec, partition);
      }
    }

    private void writePosDelete(PathOffset pathOffset) {
      positionDelete.set(pathOffset.path, pathOffset.rowOffset, null);
      posDeleteWriter.write(positionDelete);
    }

    /**
     * Write the pos-delete if there's an existing row matching the given key.
     *
     * @param key has the same columns with the equality fields.
     */
    private boolean internalPosDelete(StructLike key) {
      PathOffset previous = insertedRowMap.remove(key);

      if (previous != null) {
        // TODO attach the previous row if has a positional-delete row schema in appender factory.
        writePosDelete(previous);
        return true;
      }

      return false;
    }

    /**
     * Delete those rows whose equality fields has the same values with the given row. It will write
     * the entire row into the equality-delete file.
     *
     * @param row the given row to delete.
     */
    public void delete(T row) throws IOException {
      if (!internalPosDelete(structProjection.wrap(asStructLike(row)))) {
        eqDeleteWriter.write(row);
      }
    }

    /**
     * Delete those rows with the given key. It will only write the values of equality fields into
     * the equality-delete file.
     *
     * @param key is the projected data whose columns are the same as the equality fields.
     */
    public void deleteKey(T key) throws IOException {
      if (!internalPosDelete(asStructLikeKey(key))) {
        eqDeleteWriter.write(key);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        // Close data writer and add completed data files.
        if (dataWriter != null) {
          try {
            dataWriter.close();
          } finally {
            dataWriter = null;
          }
        }

        // Close eq-delete writer and add completed equality-delete files.
        if (eqDeleteWriter != null) {
          try {
            eqDeleteWriter.close();
          } finally {
            eqDeleteWriter = null;
          }
        }

        if (insertedRowMap != null) {
          insertedRowMap.clear();
          insertedRowMap = null;
        }

        // Add the completed pos-delete files.
        if (posDeleteWriter != null) {
          try {
            // complete will call close
            posDeleteWriter.close();
            DeleteWriteResult result = posDeleteWriter.result();
            completedDeleteFiles.addAll(result.deleteFiles());
            referencedDataFiles.addAll(result.referencedDataFiles());
          } finally {
            posDeleteWriter = null;
          }
        }
      } catch (IOException | RuntimeException e) {
        setFailure(e);
        throw e;
      }
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
    private final StructLike partitionKey;

    private EncryptedOutputFile currentFile = null;
    private W currentWriter = null;
    private long currentRows = 0;

    private BaseRollingWriter(StructLike partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    abstract W newWriter(EncryptedOutputFile file, StructLike partition);

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
      return currentRows % ROWS_DIVISOR == 0 && length(currentWriter) >= targetFileSize;
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

  protected class RollingFileWriter extends BaseRollingWriter<DataWriter<T>> {
    public RollingFileWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    DataWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
      return appenderFactory.newDataWriter(file, format, partitionKey);
    }

    @Override
    long length(DataWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(DataWriter<T> writer, T record) {
      writer.write(record);
    }

    @Override
    void complete(DataWriter<T> closedWriter) {
      completedDataFiles.add(closedWriter.toDataFile());
    }
  }

  protected class RollingEqDeleteWriter extends BaseRollingWriter<EqualityDeleteWriter<T>> {
    RollingEqDeleteWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    EqualityDeleteWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
      return appenderFactory.newEqDeleteWriter(file, format, partitionKey);
    }

    @Override
    long length(EqualityDeleteWriter<T> writer) {
      return writer.length();
    }

    @Override
    void write(EqualityDeleteWriter<T> writer, T record) {
      writer.write(record);
    }

    @Override
    void complete(EqualityDeleteWriter<T> closedWriter) {
      completedDeleteFiles.add(closedWriter.toDeleteFile());
    }
  }
}
