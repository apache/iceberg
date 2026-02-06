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
import java.util.Map;
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
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeUtil;

public class BaseDVFileWriter implements DVFileWriter {

  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final OutputFileFactory fileFactory;
  private final Function<String, PositionDeleteIndex> loadPreviousDeletes;
  private final Map<String, Deletes> deletesByPath = Maps.newHashMap();
  private final Map<String, BlobMetadata> blobsByPath = Maps.newHashMap();
  private DeleteWriteResult result = null;

  public BaseDVFileWriter(
      OutputFileFactory fileFactory, Function<String, PositionDeleteIndex> loadPreviousDeletes) {
    this.fileFactory = fileFactory;
    this.loadPreviousDeletes = loadPreviousDeletes;
  }

  @Override
  public void delete(String path, long pos, PartitionSpec spec, StructLike partition) {
    Deletes deletes =
        deletesByPath.computeIfAbsent(path, key -> new Deletes(path, spec, partition));
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
      List<DeleteFile> dvs = Lists.newArrayList();
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      // Only create PuffinWriter if there are deletes to write
      if (deletesByPath.isEmpty()) {
        this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
        return;
      }

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (Deletes deletes : deletesByPath.values()) {
          String path = deletes.path();
          PositionDeleteIndex positions = deletes.positions();
          PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
          if (previousPositions != null) {
            positions.merge(previousPositions);
            for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
              // only DVs and file-scoped deletes can be discarded from the table state
              if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
                rewrittenDeleteFiles.add(previousDeleteFile);
              }
            }
          }
          write(closeableWriter, deletes);
          referencedDataFiles.add(path);
        }
      }

      // DVs share the Puffin path and file size but have different offsets
      String puffinPath = writer.location();
      long puffinFileSize = writer.fileSize();

      for (String path : deletesByPath.keySet()) {
        DeleteFile dv = createDV(puffinPath, puffinFileSize, path);
        dvs.add(dv);
      }

      this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  private DeleteFile createDV(String path, long size, String referencedDataFile) {
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
    String path = deletes.path();
    PositionDeleteIndex positions = deletes.positions();
    BlobMetadata blobMetadata = writer.write(toBlob(positions, path));
    blobsByPath.put(path, blobMetadata);
  }

  private PuffinWriter newWriter() {
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
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

  private static class Deletes {
    private final String path;
    private final PositionDeleteIndex positions;
    private final PartitionSpec spec;
    private final StructLike partition;

    private Deletes(String path, PartitionSpec spec, StructLike partition) {
      this.path = path;
      this.positions = new BitmapPositionDeleteIndex();
      this.spec = spec;
      this.partition = StructLikeUtil.copy(partition);
    }

    public String path() {
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
