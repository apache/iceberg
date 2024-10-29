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
package org.apache.iceberg.deletes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * A position delete writer that is capable of handling unordered deletes without rows.
 *
 * <p>This writer keeps an in-memory bitmap of deleted positions per each seen data file and flushes
 * the result into a file when closed. This enables writing position delete files when the incoming
 * records are not ordered by file and position as required by the spec. If the incoming deletes are
 * ordered by an external process, use {@link PositionDeleteWriter} instead.
 *
 * <p>If configured, this writer can also load previous deletes using the provided function and
 * merge them with incoming ones prior to flushing the deletes into a file. Callers must ensure only
 * previous file-scoped deletes are loaded because partition-scoped deletes can apply to multiple
 * data files and can't be safely discarded.
 *
 * <p>Note this writer stores only positions. It does not store deleted records.
 */
public class SortingPositionOnlyDeleteWriter<T>
    implements FileWriter<PositionDelete<T>, DeleteWriteResult> {

  private final Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers;
  private final DeleteGranularity granularity;
  private final CharSequenceMap<PositionDeleteIndex> positionsByPath;
  private final Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes;
  private DeleteWriteResult result = null;

  public SortingPositionOnlyDeleteWriter(FileWriter<PositionDelete<T>, DeleteWriteResult> writer) {
    this(() -> writer, DeleteGranularity.PARTITION);
  }

  public SortingPositionOnlyDeleteWriter(
      Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers,
      DeleteGranularity granularity) {
    this(writers, granularity, path -> null /* no access to previous deletes */);
  }

  public SortingPositionOnlyDeleteWriter(
      Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers,
      DeleteGranularity granularity,
      Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes) {
    this.writers = writers;
    this.granularity = granularity;
    this.positionsByPath = CharSequenceMap.create();
    this.loadPreviousDeletes = loadPreviousDeletes;
  }

  @Override
  public void write(PositionDelete<T> positionDelete) {
    CharSequence path = positionDelete.path();
    long position = positionDelete.pos();
    PositionDeleteIndex positions =
        positionsByPath.computeIfAbsent(path, key -> new BitmapPositionDeleteIndex());
    positions.delete(position);
  }

  @Override
  public long length() {
    throw new UnsupportedOperationException(getClass().getName() + " does not implement length");
  }

  @Override
  public DeleteWriteResult result() {
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      switch (granularity) {
        case FILE:
          this.result = writeFileDeletes();
          return;
        case PARTITION:
          this.result = writePartitionDeletes();
          return;
        default:
          throw new UnsupportedOperationException("Unsupported delete granularity: " + granularity);
      }
    }
  }

  // write deletes for all data files together
  private DeleteWriteResult writePartitionDeletes() throws IOException {
    return writeDeletes(positionsByPath.keySet());
  }

  // write deletes for different data files into distinct delete files
  private DeleteWriteResult writeFileDeletes() throws IOException {
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
    List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

    for (CharSequence path : positionsByPath.keySet()) {
      DeleteWriteResult writeResult = writeDeletes(ImmutableList.of(path));
      deleteFiles.addAll(writeResult.deleteFiles());
      referencedDataFiles.addAll(writeResult.referencedDataFiles());
      rewrittenDeleteFiles.addAll(writeResult.rewrittenDeleteFiles());
    }

    return new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  private DeleteWriteResult writeDeletes(Collection<CharSequence> paths) throws IOException {
    if (paths.isEmpty()) {
      return new DeleteWriteResult(Lists.newArrayList(), CharSequenceSet.empty());
    }

    FileWriter<PositionDelete<T>, DeleteWriteResult> writer = writers.get();
    List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

    try {
      PositionDelete<T> positionDelete = PositionDelete.create();
      for (CharSequence path : sort(paths)) {
        PositionDeleteIndex positions = positionsByPath.get(path);
        PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
        if (previousPositions != null && previousPositions.isNotEmpty()) {
          validatePreviousDeletes(previousPositions);
          positions.merge(previousPositions);
          rewrittenDeleteFiles.addAll(previousPositions.deleteFiles());
        }
        positions.forEach(position -> writer.write(positionDelete.set(path, position)));
      }
    } finally {
      writer.close();
    }

    DeleteWriteResult writerResult = writer.result();
    List<DeleteFile> deleteFiles = writerResult.deleteFiles();
    CharSequenceSet referencedDataFiles = writerResult.referencedDataFiles();
    return new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
  }

  private void validatePreviousDeletes(PositionDeleteIndex index) {
    Preconditions.checkArgument(
        index.deleteFiles().stream().allMatch(this::isFileScoped),
        "Previous deletes must be file-scoped");
  }

  private boolean isFileScoped(DeleteFile deleteFile) {
    return ContentFileUtil.referencedDataFile(deleteFile) != null;
  }

  private Collection<CharSequence> sort(Collection<CharSequence> paths) {
    if (paths.size() <= 1) {
      return paths;
    } else {
      List<CharSequence> sortedPaths = Lists.newArrayList(paths);
      sortedPaths.sort(Comparators.charSequences());
      return sortedPaths;
    }
  }
}
