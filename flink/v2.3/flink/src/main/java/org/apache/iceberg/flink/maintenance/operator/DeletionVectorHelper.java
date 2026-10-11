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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * Finds, reads and writes the deletion vectors of a table, for operators that turn resolved row
 * positions into deletion vectors.
 *
 * <p>A data file may carry at most one deletion vector, so new positions always have to be merged
 * with the vector the file already carries, and that vector has to be replaced in the same commit.
 * {@link #collectExistingDVs} looks up the vectors the files carry and {@link #write} writes the
 * merged ones.
 */
@Internal
public class DeletionVectorHelper {

  private final Table table;
  private final DeleteLoader deleteLoader;
  private int manifestsRead;

  public DeletionVectorHelper(Table table) {
    this.table = table;
    this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
  }

  /**
   * Returns the deletion vector each of the given data files carries in the given snapshot.
   *
   * <p>A deletion vector inherits the spec and partition of the data file it references, so delete
   * manifests whose partition summaries cannot cover the partitions of the given files are skipped.
   *
   * @param snapshot the snapshot to look in, or null for an empty branch
   * @param files the data files to look up, by path
   */
  public Map<String, DeleteFile> collectExistingDVs(
      Snapshot snapshot, Map<String, FilePositions> files) {
    this.manifestsRead = 0;
    Map<String, DeleteFile> found = Maps.newHashMap();
    if (snapshot == null || files.isEmpty()) {
      return found;
    }

    // Prune delete manifests whose partition summaries cannot cover the cycle's affected
    // partitions. A DV inherits its referenced data file's spec and partition, so partition pruning
    // works for DV manifests.
    Map<Integer, ManifestEvaluator> evaluators = partitionEvaluators(files);
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      ManifestEvaluator evaluator = evaluators.get(manifest.partitionSpecId());
      if (evaluator != null && evaluator.eval(manifest)) {
        readDeletionVectors(manifest, files.keySet(), found);
      }
    }

    return found;
  }

  /** Number of delete manifests the last {@link #collectExistingDVs} call read. */
  public int manifestsReadLastLookup() {
    return manifestsRead;
  }

  /**
   * Writes one deletion vector per data file, holding the given positions merged with the vector
   * the file already carries. The result lists the replaced vectors as rewritten delete files.
   *
   * @param fileFactory creates the Puffin file the vectors are written to
   * @param files the positions to delete, by data file path
   * @param attachedVectors the vector each data file carries now, see {@link #collectExistingDVs}
   */
  public DeleteWriteResult write(
      OutputFileFactory fileFactory,
      Map<String, FilePositions> files,
      Map<String, DeleteFile> attachedVectors) {
    BaseDVFileWriter writer =
        new BaseDVFileWriter(fileFactory, path -> loadAttached(path, attachedVectors));
    try (BaseDVFileWriter closeable = writer) {
      for (Map.Entry<String, FilePositions> entry : files.entrySet()) {
        String dataFilePath = entry.getKey();
        FilePositions file = entry.getValue();
        PartitionSpec spec = table.specs().get(file.specId());
        StructLike partition = file.partition(spec);
        file.positions()
            .forEach((long position) -> closeable.delete(dataFilePath, position, spec, partition));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write deletion vectors for " + table.name(), e);
    }

    return writer.result();
  }

  /** Reads the positions a deletion vector removes from the data file it references. */
  public PositionDeleteIndex load(DeleteFile deletionVector) {
    return deleteLoader.loadPositionDeletes(
        ImmutableList.of(deletionVector), deletionVector.referencedDataFile());
  }

  /**
   * Reads the positions the given deletion vectors remove from a data file.
   *
   * @return the deleted positions, or null when {@code deletionVectors} is empty
   */
  public PositionDeleteIndex load(String dataFilePath, List<DeleteFile> deletionVectors) {
    if (deletionVectors.isEmpty()) {
      return null;
    }

    return deleteLoader.loadPositionDeletes(deletionVectors, dataFilePath);
  }

  private PositionDeleteIndex loadAttached(
      String dataFilePath, Map<String, DeleteFile> attachedVectors) {
    DeleteFile deletionVector = attachedVectors.get(dataFilePath);
    return deletionVector != null ? load(deletionVector) : null;
  }

  private Map<Integer, ManifestEvaluator> partitionEvaluators(Map<String, FilePositions> files) {
    Map<Integer, StructLikeWrapper> templatesBySpec = Maps.newHashMap();
    Map<Integer, Set<StructLikeWrapper>> partitionsBySpec = Maps.newHashMap();
    for (FilePositions file : files.values()) {
      PartitionSpec spec = table.specs().get(file.specId());
      StructLikeWrapper template =
          templatesBySpec.computeIfAbsent(
              file.specId(), id -> StructLikeWrapper.forType(spec.partitionType()));
      partitionsBySpec
          .computeIfAbsent(file.specId(), id -> Sets.newHashSet())
          .add(template.copyFor(file.partition(spec)));
    }

    Map<Integer, ManifestEvaluator> evaluators = Maps.newHashMap();
    for (Map.Entry<Integer, Set<StructLikeWrapper>> entry : partitionsBySpec.entrySet()) {
      PartitionSpec spec = table.specs().get(entry.getKey());
      Expression filter = partitionFilter(spec, entry.getValue());
      evaluators.put(entry.getKey(), ManifestEvaluator.forPartitionFilter(filter, spec, false));
    }

    return evaluators;
  }

  private static Expression partitionFilter(PartitionSpec spec, Set<StructLikeWrapper> partitions) {
    List<PartitionField> fields = spec.fields();
    Expression anyPartition = Expressions.alwaysFalse();
    for (StructLikeWrapper wrapper : partitions) {
      StructLike partition = wrapper.get();
      Expression onePartition = Expressions.alwaysTrue();
      for (int i = 0; i < fields.size(); i++) {
        String name = fields.get(i).name();
        Object value = partition.get(i, Object.class);
        Expression predicate =
            value == null ? Expressions.isNull(name) : Expressions.equal(name, value);
        onePartition = Expressions.and(onePartition, predicate);
      }

      anyPartition = Expressions.or(anyPartition, onePartition);
    }

    return anyPartition;
  }

  private void readDeletionVectors(
      ManifestFile manifest, Set<String> dataFilePaths, Map<String, DeleteFile> found) {
    manifestsRead++;
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
      for (DeleteFile deleteFile : reader) {
        if (ContentFileUtil.isDV(deleteFile)
            && deleteFile.referencedDataFile() != null
            && dataFilePaths.contains(deleteFile.referencedDataFile())) {
          found.put(deleteFile.referencedDataFile(), deleteFile);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read manifest: " + manifest.path(), e);
    }
  }

  /** Positions to delete from one data file, with the spec and partition needed to write them. */
  @Internal
  public static final class FilePositions {
    private final int specId;
    private final byte[] encodedPartition;
    private final Roaring64Bitmap positions = new Roaring64Bitmap();
    private StructLike partition;

    /**
     * @param specId spec of the data file
     * @param encodedPartition partition of the data file, see {@link
     *     StructLikeSerializer#encodePartition}
     */
    public FilePositions(int specId, byte[] encodedPartition) {
      this.specId = specId;
      this.encodedPartition = encodedPartition;
    }

    /** Creates an instance for a data file whose partition is already decoded. */
    public static FilePositions forPartition(int specId, StructLike partition) {
      FilePositions file = new FilePositions(specId, null);
      file.partition = partition;
      return file;
    }

    public int specId() {
      return specId;
    }

    public StructLike partition(PartitionSpec spec) {
      if (partition == null) {
        partition = StructLikeSerializer.decodePartition(encodedPartition, spec.partitionType());
      }

      return partition;
    }

    public Roaring64Bitmap positions() {
      return positions;
    }

    public void add(long position) {
      positions.addLong(position);
    }
  }
}
