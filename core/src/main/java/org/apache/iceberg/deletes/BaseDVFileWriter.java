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
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.StructCopy;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;

public class BaseDVFileWriter implements DVFileWriter {

  private static final String REF_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final OutputFileFactory fileFactory;
  private final Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes;
  private final CharSequenceMap<Deletes> deletesByPath = CharSequenceMap.create();
  private final CharSequenceMap<BlobMetadata> blobsByPath = CharSequenceMap.create();
  private DeleteWriteResult result = null;

  public BaseDVFileWriter(
      OutputFileFactory fileFactory,
      Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes) {
    this.fileFactory = fileFactory;
    this.loadPreviousDeletes = loadPreviousDeletes;
  }

  @Override
  public void write(CharSequence path, long pos, PartitionSpec spec, StructLike partition) {
    Deletes deletes = deletesByPath.computeIfAbsent(path, () -> new Deletes(path, spec, partition));
    PositionDeleteIndex positions = deletes.positions();
    positions.delete(pos);
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      List<DeleteFile> deleteFiles = Lists.newArrayList();
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (Deletes deletes : deletesByPath.values()) {
          CharSequence path = deletes.path();
          PositionDeleteIndex positions = deletes.positions();
          PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
          if (previousPositions != null) {
            positions.merge(previousPositions);
            for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
              // only file-scoped deletes can be discarded from the table state
              if (ContentFileUtil.referencedDataFile(previousDeleteFile) != null) {
                rewrittenDeleteFiles.add(previousDeleteFile);
              }
            }
          }
          write(closeableWriter, deletes);
          referencedDataFiles.add(path);
        }
      }

      // all delete files share the same Puffin path but have different content offsets
      String puffinPath = writer.path();
      long puffinFileSize = writer.fileSize();

      for (CharSequence path : deletesByPath.keySet()) {
        DeleteFile deleteFile = createDeleteFile(puffinPath, puffinFileSize, path);
        deleteFiles.add(deleteFile);
      }

      this.result = new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  private DeleteFile createDeleteFile(String path, long size, CharSequence referencedDataFile) {
    Deletes deletes = deletesByPath.get(referencedDataFile);
    BlobMetadata blobMetadata = blobsByPath.get(referencedDataFile);
    return FileMetadata.deleteFileBuilder(deletes.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(path)
        .withPartition(deletes.partition())
        .withFileSizeInBytes(size)
        .withReferencedDataFile(referencedDataFile)
        .withContentOffset(blobMetadata.offset())
        .withContentSizeInBytes(blobMetadata.length())
        .withRecordCount(deletes.positions().cardinality())
        .build();
  }

  private void write(PuffinWriter writer, Deletes deletes) {
    CharSequence path = deletes.path();
    PositionDeleteIndex positions = deletes.positions();
    BlobMetadata blobMetadata = writer.write(toBlob(positions, path));
    blobsByPath.put(path, blobMetadata);
  }

  private PuffinWriter newWriter() {
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    String ident = "Iceberg " + IcebergBuild.fullVersion();
    return Puffin.write(outputFile).createdBy(ident).build();
  }

  private Blob toBlob(PositionDeleteIndex positions, CharSequence path) {
    return new Blob(
        StandardBlobTypes.DELETE_VECTOR_V1,
        ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId()),
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        positions.serialize(),
        null /* uncompressed */,
        ImmutableMap.of(
            REF_DATA_FILE_KEY,
            path.toString(),
            CARDINALITY_KEY,
            String.valueOf(positions.cardinality())));
  }

  private static class Deletes {
    private final CharSequence path;
    private final PositionDeleteIndex positions;
    private final PartitionSpec spec;
    private final StructLike partition;

    private Deletes(CharSequence path, PartitionSpec spec, StructLike partition) {
      this.path = path;
      this.positions = new BitmapPositionDeleteIndex();
      this.spec = spec;
      this.partition = StructCopy.copy(partition);
    }

    public CharSequence path() {
      return path;
    }

    public PositionDeleteIndex positions() {
      return positions;
    }

    public PartitionSpec spec() {
      return spec;
    }

    public StructLike partition() {
      return partition;
    }
  }
}
