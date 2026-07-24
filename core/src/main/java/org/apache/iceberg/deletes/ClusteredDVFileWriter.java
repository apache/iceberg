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
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeUtil;

/**
 * A {@link DVFileWriter} for inputs that are clustered by data file.
 *
 * <p>{@link BaseDVFileWriter} buffers one in-memory position index per data file the task deletes
 * from and merges each file's previous deletes only on close, so its peak working set is
 * proportional to the number of data files touched by the task (which, for hot-key MERGE workloads,
 * grows with the table). When the incoming deletes are clustered by data file - e.g. Spark position
 * delta writes request ordering by spec, partition, file and position when fanout writers are
 * disabled - that buffering is unnecessary: this writer keeps a single live position index. On the
 * first delete of the next data file, the previous file's index is merged with that file's existing
 * deletes, flushed as a blob into the shared Puffin file and released, so the resident state is one
 * position index plus small per-file blob metadata needed to create the {@link DeleteFile} entries
 * on close.
 *
 * <p>The writer fails fast if the input revisits a data file after moving on to another one, as
 * that would produce multiple DVs for one data file in a single commit.
 */
public class ClusteredDVFileWriter implements DVFileWriter {

  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final Supplier<OutputFile> dvOutputFile;
  private final Function<String, PositionDeleteIndex> loadPreviousDeletes;
  private final List<FlushedDeletes> flushedDeletes = Lists.newArrayList();
  private final Set<String> seenPaths = Sets.newHashSet();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private final List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

  private PuffinWriter writer = null;
  private String currentPath = null;
  private PositionDeleteIndex currentPositions = null;
  private PartitionSpec currentSpec = null;
  private StructLike currentPartition = null;
  private DeleteWriteResult result = null;

  public ClusteredDVFileWriter(
      OutputFileFactory fileFactory, Function<String, PositionDeleteIndex> loadPreviousDeletes) {
    this(() -> fileFactory.newOutputFile().encryptingOutputFile(), loadPreviousDeletes);
  }

  public ClusteredDVFileWriter(
      Supplier<OutputFile> dvOutputFile,
      Function<String, PositionDeleteIndex> loadPreviousDeletes) {
    this.dvOutputFile = dvOutputFile;
    this.loadPreviousDeletes = loadPreviousDeletes;
  }

  @Override
  public void delete(String path, long pos, PartitionSpec spec, StructLike partition) {
    if (!path.equals(currentPath)) {
      startNewFile(path, spec, partition);
    }

    currentPositions.delete(pos);
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      flushCurrentFile();

      List<DeleteFile> dvs = Lists.newArrayList();

      if (writer != null) {
        writer.close();

        // DVs share the Puffin path and file size but have different offsets
        String puffinPath = writer.location();
        long puffinFileSize = writer.fileSize();

        for (FlushedDeletes deletes : flushedDeletes) {
          dvs.add(createDV(puffinPath, puffinFileSize, deletes));
        }
      }

      this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  private void startNewFile(String path, PartitionSpec spec, StructLike partition) {
    Preconditions.checkState(
        seenPaths.add(path),
        "Incoming deletes are not clustered by data file: %s was already completed",
        path);

    flushCurrentFile();

    this.currentPath = path;
    this.currentPositions = new BitmapPositionDeleteIndex();
    this.currentSpec = spec;
    this.currentPartition = StructLikeUtil.copy(partition);
  }

  private void flushCurrentFile() {
    if (currentPath == null) {
      return;
    }

    PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(currentPath);
    if (previousPositions != null) {
      currentPositions.merge(previousPositions);
      for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
        // only DVs and file-scoped deletes can be discarded from the table state
        if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
          rewrittenDeleteFiles.add(previousDeleteFile);
        }
      }
    }

    if (writer == null) {
      this.writer = newWriter();
    }

    BlobMetadata blobMetadata = writer.write(toBlob(currentPositions, currentPath));
    flushedDeletes.add(
        new FlushedDeletes(
            currentPath,
            currentSpec,
            currentPartition,
            blobMetadata,
            currentPositions.cardinality()));
    referencedDataFiles.add(currentPath);

    // release the bitmap - this is the whole point of the clustered writer
    this.currentPath = null;
    this.currentPositions = null;
    this.currentSpec = null;
    this.currentPartition = null;
  }

  private DeleteFile createDV(String path, long size, FlushedDeletes deletes) {
    return FileMetadata.deleteFileBuilder(deletes.spec)
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(path)
        .withPartition(deletes.partition)
        .withFileSizeInBytes(size)
        .withReferencedDataFile(deletes.path)
        .withContentOffset(deletes.blobMetadata.offset())
        .withContentSizeInBytes(deletes.blobMetadata.length())
        .withRecordCount(deletes.cardinality)
        .build();
  }

  private PuffinWriter newWriter() {
    OutputFile outputFile = dvOutputFile.get();
    return Puffin.write(outputFile).createdBy(IcebergBuild.fullVersion()).build();
  }

  private Blob toBlob(PositionDeleteIndex positions, String path) {
    return new Blob(
        StandardBlobTypes.DV_V1,
        ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId()),
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        positions.serialize(),
        null /* uncompressed */,
        ImmutableMap.of(
            REFERENCED_DATA_FILE_KEY,
            path,
            CARDINALITY_KEY,
            String.valueOf(positions.cardinality())));
  }

  private static class FlushedDeletes {
    private final String path;
    private final PartitionSpec spec;
    private final StructLike partition;
    private final BlobMetadata blobMetadata;
    private final long cardinality;

    private FlushedDeletes(
        String path,
        PartitionSpec spec,
        StructLike partition,
        BlobMetadata blobMetadata,
        long cardinality) {
      this.path = path;
      this.spec = spec;
      this.partition = partition;
      this.blobMetadata = blobMetadata;
      this.cardinality = cardinality;
    }
  }
}
