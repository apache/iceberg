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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.FileScopedPositionDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.CharSequenceWrapper;

class SortedPosDeleteWriter<T> implements FileWriter<PositionDelete<T>, DeleteWriteResult> {
  private static final long DEFAULT_RECORDS_NUM_THRESHOLD = 100_000L;

  private final Map<CharSequenceWrapper, List<PosRow<T>>> posDeletes = Maps.newHashMap();
  private final CharSequenceWrapper wrapper = CharSequenceWrapper.wrap(null);

  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileFormat format;
  private final StructLike partition;
  private final long recordsNumThreshold;
  private final DeleteGranularity deleteGranularity;

  private DeleteWriteResult result;
  private int records = 0;
  private boolean closed = false;
  private Throwable failure;

  SortedPosDeleteWriter(
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileFormat format,
      StructLike partition,
      long recordsNumThreshold,
      DeleteGranularity deleteGranularity) {
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.format = format;
    this.partition = partition;
    this.recordsNumThreshold = recordsNumThreshold;
    this.deleteGranularity = deleteGranularity;
  }

  SortedPosDeleteWriter(
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileFormat format,
      StructLike partition,
      long recordsNumThreshold) {
    this(
        appenderFactory,
        fileFactory,
        format,
        partition,
        recordsNumThreshold,
        DeleteGranularity.PARTITION);
  }

  SortedPosDeleteWriter(
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileFormat format,
      StructLike partition,
      DeleteGranularity deleteGranularity) {
    this(
        appenderFactory,
        fileFactory,
        format,
        partition,
        DEFAULT_RECORDS_NUM_THRESHOLD,
        deleteGranularity);
  }

  SortedPosDeleteWriter(
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileFormat format,
      StructLike partition) {
    this(
        appenderFactory,
        fileFactory,
        format,
        partition,
        DEFAULT_RECORDS_NUM_THRESHOLD,
        DeleteGranularity.PARTITION);
  }

  protected void setFailure(Throwable throwable) {
    if (failure == null) {
      this.failure = throwable;
    }
  }

  @Override
  public long length() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement length");
  }

  @Override
  public void write(PositionDelete<T> payload) {
    delete(payload.path(), payload.pos(), payload.row());
  }

  public void delete(CharSequence path, long pos) {
    delete(path, pos, null);
  }

  public void delete(CharSequence path, long pos, T row) {
    List<PosRow<T>> posRows = posDeletes.get(wrapper.set(path));
    if (posRows != null) {
      posRows.add(PosRow.of(pos, row));
    } else {
      posDeletes.put(CharSequenceWrapper.wrap(path), Lists.newArrayList(PosRow.of(pos, row)));
    }

    records += 1;

    // TODO Flush buffer based on the policy that checking whether whole heap memory size exceed the
    // threshold.
    if (records >= recordsNumThreshold) {
      flushDeletes();
    }
  }

  public List<DeleteFile> complete() throws IOException {
    close();

    Preconditions.checkState(failure == null, "Cannot return results from failed writer", failure);

    return result.deleteFiles();
  }

  public CharSequenceSet referencedDataFiles() {
    return result.referencedDataFiles();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.closed = true;
      flushDeletes();
    }
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return result;
  }

  private void flushDeletes() {
    if (posDeletes.isEmpty()) {
      result = new DeleteWriteResult(ImmutableList.of(), CharSequenceSet.empty());
      return;
    }

    FileWriter<PositionDelete<T>, DeleteWriteResult> writer =
        DeleteGranularity.FILE.equals(deleteGranularity)
            ? new FileScopedPositionDeleteWriter<>(this::createWriter)
            : createWriter();
    PositionDelete<T> posDelete = PositionDelete.create();
    try (FileWriter<PositionDelete<T>, DeleteWriteResult> closeableWriter = writer) {
      // Sort all the paths.
      List<CharSequence> paths = Lists.newArrayListWithCapacity(posDeletes.keySet().size());
      for (CharSequenceWrapper charSequenceWrapper : posDeletes.keySet()) {
        paths.add(charSequenceWrapper.get());
      }
      paths.sort(Comparators.charSequences());

      // Write all the sorted <path, pos, row> triples.
      for (CharSequence path : paths) {
        List<PosRow<T>> positions = posDeletes.get(wrapper.set(path));
        positions.sort(Comparator.comparingLong(PosRow::pos));

        positions.forEach(
            posRow -> closeableWriter.write(posDelete.set(path, posRow.pos(), posRow.row())));
      }
    } catch (IOException e) {
      setFailure(e);
      throw new UncheckedIOException(
          "Failed to write the sorted path/pos pairs to pos-delete file", e);
    }

    // Clear the buffered pos-deletions.
    posDeletes.clear();
    records = 0;

    result = writer.result();
  }

  private FileWriter<PositionDelete<T>, DeleteWriteResult> createWriter() {
    return appenderFactory.newPosDeleteWriter(
        partition == null ? fileFactory.newOutputFile() : fileFactory.newOutputFile(partition),
        format,
        partition);
  }

  private static class PosRow<R> {
    private final long pos;
    private final R row;

    static <R> PosRow<R> of(long pos, R row) {
      return new PosRow<>(pos, row);
    }

    private PosRow(long pos, R row) {
      this.pos = pos;
      this.row = row;
    }

    long pos() {
      return pos;
    }

    R row() {
      return row;
    }
  }
}
