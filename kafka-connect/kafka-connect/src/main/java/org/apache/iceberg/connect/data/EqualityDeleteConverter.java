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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructLikeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves equality delete files against the rows of a table and writes the matching row positions
 * as deletion vectors, so the equality delete files never have to be committed.
 *
 * <p>An equality delete written by the sink deletes every row of an earlier commit whose identifier
 * fields equal one of the deleted keys. The converter reads the keys, groups them by equality
 * fields and partition, and asks a {@link KeyPositionResolver} for the rows that hold them. The
 * default resolver, {@link ScanKeyPositionResolver}, scans the data files that the column
 * statistics cannot rule out; an index-backed resolver can be plugged in instead. A data file that
 * already has a deletion vector gets a merged one; the replaced vector is reported in {@link
 * Result#rewrittenDvFiles()} and must be removed by the commit.
 *
 * <p>The positions are only valid for the snapshot they were resolved against, see {@link
 * Result#baseSnapshotId()}. The commit that adds the deletion vectors has to validate that no
 * conflicting data or delete files were added since then and re-run the conversion otherwise.
 * {@link Result#conflictFilter()} narrows that validation to the deleted keys.
 *
 * <p>Requires format version 3.
 */
public class EqualityDeleteConverter {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteConverter.class);

  private final Table table;
  private final String branch;
  private final KeyPositionResolver resolver;
  private final DeleteLoader deleteLoader;
  private final AtomicInteger scannedDataFiles = new AtomicInteger();

  /**
   * Creates a converter that finds the rows by scanning data files.
   *
   * @param table the table to resolve the deletes against
   * @param branch the branch that is written, or null for the main branch
   */
  public EqualityDeleteConverter(Table table, String branch) {
    this(table, branch, new ScanKeyPositionResolver(table));
  }

  EqualityDeleteConverter(Table table, String branch, KeyPositionResolver resolver) {
    this.table = table;
    this.branch = branch;
    this.resolver = resolver;
    this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
  }

  /**
   * Converts the given equality delete files into deletion vectors on the current snapshot of the
   * branch. The table is expected to be refreshed by the caller.
   */
  public Result convert(List<DeleteFile> eqDeleteFiles) {
    Snapshot base = branch == null ? table.currentSnapshot() : table.snapshot(branch);
    if (base == null) {
      // there are no rows an equality delete could remove
      LOG.info(
          "Table {} has no snapshot on branch {}, {} equality delete file(s) match no rows",
          table.name(),
          branch == null ? "main" : branch,
          eqDeleteFiles.size());
      return Result.empty();
    }

    Map<DeleteGroup, List<DeleteFile>> groups = groupDeleteFiles(eqDeleteFiles);

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(FileFormat.PUFFIN)
            .build();
    // deletion vectors already attached to a matched data file, folded into the new vector
    Map<String, PositionDeleteIndex> previousDeletes = Maps.newHashMap();
    BaseDVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, previousDeletes::get);

    Expression conflictFilter = Expressions.alwaysFalse();
    int comparisonsPerFile = KeyFilters.comparisonsPerFile(base);
    long matchedRows = 0L;
    long deletedKeys = 0L;
    scannedDataFiles.set(0);

    long start = System.currentTimeMillis();
    try {
      for (Map.Entry<DeleteGroup, List<DeleteFile>> entry : groups.entrySet()) {
        DeleteGroup group = entry.getKey();
        Schema keySchema = keySchema(group.equalityFieldIds());
        StructLikeSet keys = deleteLoader.loadEqualityDeletes(entry.getValue(), keySchema);
        if (keys.isEmpty()) {
          continue;
        }

        deletedKeys += keys.size();
        conflictFilter =
            Expressions.or(
                conflictFilter,
                Expressions.and(
                    KeyFilters.partitionFilter(table.schema(), group.spec(), group.partition()),
                    KeyFilters.keyFilter(keySchema, keys, comparisonsPerFile)));

        KeyPositionResolver.Resolution resolution =
            resolver.resolve(base, keySchema, keys, group.spec(), group.partition());
        scannedDataFiles.addAndGet(resolution.scannedDataFiles());

        for (KeyPositionResolver.FileMatches matches : resolution.matches()) {
          if (matches.existingDeletes() != null) {
            previousDeletes.put(matches.path(), matches.existingDeletes());
          }

          long[] count = new long[1];
          matches.forEachPosition(
              pos -> {
                dvWriter.delete(matches.path(), pos, matches.spec(), matches.partition());
                count[0]++;
              });
          matchedRows += count[0];
        }
      }

      dvWriter.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    DeleteWriteResult writeResult = dvWriter.result();
    LOG.info(
        "Converted {} equality delete file(s) with {} key(s) into {} deletion vector(s) covering {} row(s) "
            + "of table {} at snapshot {}, scanned {} data file(s) in {} ms",
        eqDeleteFiles.size(),
        deletedKeys,
        writeResult.deleteFiles().size(),
        matchedRows,
        table.name(),
        base.snapshotId(),
        scannedDataFiles.get(),
        System.currentTimeMillis() - start);

    return new Result(
        base.snapshotId(),
        writeResult.deleteFiles(),
        writeResult.rewrittenDeleteFiles(),
        conflictFilter);
  }

  @VisibleForTesting
  int scannedDataFiles() {
    return scannedDataFiles.get();
  }

  /**
   * Groups the delete files by equality field set and partition. A partitioned equality delete only
   * applies to its own partition, and a smaller key set per group prunes better.
   */
  private Map<DeleteGroup, List<DeleteFile>> groupDeleteFiles(List<DeleteFile> eqDeleteFiles) {
    Map<DeleteGroup, List<DeleteFile>> groups = Maps.newLinkedHashMap();
    for (DeleteFile deleteFile : eqDeleteFiles) {
      Preconditions.checkArgument(
          deleteFile.content() == FileContent.EQUALITY_DELETES,
          "Not an equality delete file: %s",
          deleteFile.location());
      PartitionSpec spec = table.specs().get(deleteFile.specId());
      Preconditions.checkArgument(
          spec != null,
          "Unknown partition spec %s for %s",
          deleteFile.specId(),
          deleteFile.location());
      DeleteGroup group =
          new DeleteGroup(
              ImmutableSet.copyOf(deleteFile.equalityFieldIds()), spec, deleteFile.partition());
      groups.computeIfAbsent(group, key -> Lists.newArrayList()).add(deleteFile);
    }

    return groups;
  }

  private Schema keySchema(Set<Integer> equalityFieldIds) {
    Schema keySchema = TypeUtil.select(table.schema(), equalityFieldIds);
    Preconditions.checkArgument(
        TypeUtil.getProjectedIds(keySchema).containsAll(equalityFieldIds),
        "Equality field IDs %s are not all present in table schema %s",
        equalityFieldIds,
        table.schema());
    return keySchema;
  }

  /** Outcome of a conversion, to be committed together with the data files of the batch. */
  public static class Result {
    private static final Result EMPTY =
        new Result(null, ImmutableList.of(), ImmutableList.of(), Expressions.alwaysFalse());

    private final Long baseSnapshotId;
    private final List<DeleteFile> dvFiles;
    private final List<DeleteFile> rewrittenDvFiles;
    private final Expression conflictFilter;

    Result(
        Long baseSnapshotId,
        List<DeleteFile> dvFiles,
        List<DeleteFile> rewrittenDvFiles,
        Expression conflictFilter) {
      this.baseSnapshotId = baseSnapshotId;
      this.dvFiles = ImmutableList.copyOf(dvFiles);
      this.rewrittenDvFiles = ImmutableList.copyOf(rewrittenDvFiles);
      this.conflictFilter = conflictFilter;
    }

    static Result empty() {
      return EMPTY;
    }

    /** Snapshot the positions were resolved against, or null when the branch had no snapshot. */
    public Long baseSnapshotId() {
      return baseSnapshotId;
    }

    /** Deletion vectors to add. */
    public List<DeleteFile> dvFiles() {
      return dvFiles;
    }

    /** Deletion vectors that were merged into a new one and must be removed. */
    public List<DeleteFile> rewrittenDvFiles() {
      return rewrittenDvFiles;
    }

    /**
     * Row filter covering every deleted key, for detecting data or delete files that were added
     * concurrently for those keys.
     */
    public Expression conflictFilter() {
      return conflictFilter;
    }
  }

  /** Equality delete files that share the equality fields and the partition. */
  private static class DeleteGroup {
    private final Set<Integer> equalityFieldIds;
    private final PartitionSpec spec;
    private final StructLikeWrapper partition;

    DeleteGroup(Set<Integer> equalityFieldIds, PartitionSpec spec, StructLike partition) {
      this.equalityFieldIds = equalityFieldIds;
      this.spec = spec;
      this.partition = StructLikeWrapper.forType(spec.partitionType()).set(partition);
    }

    Set<Integer> equalityFieldIds() {
      return equalityFieldIds;
    }

    PartitionSpec spec() {
      return spec;
    }

    StructLike partition() {
      return partition.get();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (!(other instanceof DeleteGroup)) {
        return false;
      }

      DeleteGroup that = (DeleteGroup) other;
      return equalityFieldIds.equals(that.equalityFieldIds)
          && spec.specId() == that.spec.specId()
          && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(equalityFieldIds, spec.specId(), partition);
    }
  }
}
