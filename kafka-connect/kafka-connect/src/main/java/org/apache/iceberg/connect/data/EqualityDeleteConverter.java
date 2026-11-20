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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
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
 * fields equal one of the deleted keys. The converter finds those rows by reading the data files
 * that their partition values and column statistics cannot rule out. A data file that already has a
 * deletion vector gets a merged one; the replaced vector is reported in {@link
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

  /**
   * Creates a converter that finds the rows by scanning data files.
   *
   * @param table the table to resolve the deletes against
   * @param branch the branch that is written, or null for the main branch
   * @throws IllegalArgumentException if the table's format version is lower than 3
   */
  public EqualityDeleteConverter(Table table, String branch) {
    this(table, branch, new ScanKeyPositionResolver(table));
  }

  EqualityDeleteConverter(Table table, String branch, KeyPositionResolver resolver) {
    int formatVersion = TableUtil.formatVersion(table);
    Preconditions.checkArgument(
        formatVersion >= 3,
        "Cannot convert equality deletes of table %s: deletion vectors require format version 3, "
            + "found %s",
        table.name(),
        formatVersion);
    this.table = table;
    this.branch = branch;
    this.resolver = resolver;
    this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
  }

  /**
   * Converts the given equality delete files into deletion vectors on the current snapshot of the
   * branch. A branch that does not exist yet is resolved against the current snapshot of main,
   * which a commit creating the branch builds on. The table is expected to be refreshed by the
   * caller.
   */
  public Result convert(List<DeleteFile> eqDeleteFiles) {
    Snapshot base =
        branch == null || !table.refs().containsKey(branch)
            ? table.currentSnapshot()
            : table.snapshot(branch);
    long start = System.currentTimeMillis();

    // the conflict filter covers the keys even when the branch has no snapshot, so that the commit
    // detects rows another writer commits to the branch first
    Map<DeleteGroup, StructLikeSet> keysByGroup = Maps.newLinkedHashMap();
    Expression conflictFilter = Expressions.alwaysFalse();
    long deletedKeys = 0L;
    for (Map.Entry<DeleteGroup, List<DeleteFile>> entry :
        groupDeleteFiles(eqDeleteFiles).entrySet()) {
      DeleteGroup group = entry.getKey();
      Schema keySchema = keySchema(group.equalityFieldIds());
      StructLikeSet keys = deleteLoader.loadEqualityDeletes(entry.getValue(), keySchema);
      if (keys.isEmpty()) {
        continue;
      }

      keysByGroup.put(group, keys);
      deletedKeys += keys.size();
      conflictFilter =
          Expressions.or(
              conflictFilter,
              Expressions.and(
                  KeyFilters.partitionFilter(table.schema(), group.spec(), group.partition()),
                  KeyFilters.keyFilter(keySchema, keys)));
    }

    if (base == null) {
      LOG.info(
          "Table {} has no snapshot on branch {}, {} equality delete file(s) with {} key(s) "
              + "match no rows",
          table.name(),
          branch == null ? "main" : branch,
          eqDeleteFiles.size(),
          deletedKeys);
      return new Result(null, ImmutableList.of(), ImmutableList.of(), conflictFilter, 0);
    }

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(FileFormat.PUFFIN)
            .build();
    // position deletes a matched data file already has, merged into its new deletion vector
    Map<String, PositionDeleteIndex> previousDeletes = Maps.newHashMap();
    BaseDVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, previousDeletes::get);

    long matchedRows = 0L;
    int plannedDataFiles = 0;
    try {
      for (Map.Entry<DeleteGroup, StructLikeSet> entry : keysByGroup.entrySet()) {
        DeleteGroup group = entry.getKey();
        KeyPositionResolver.Resolution resolution =
            resolver.resolve(
                base,
                keySchema(group.equalityFieldIds()),
                entry.getValue(),
                group.spec(),
                group.partition());
        plannedDataFiles += resolution.plannedDataFiles();

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
            + "of table {} at snapshot {}, planned {} data file(s) in {} ms",
        eqDeleteFiles.size(),
        deletedKeys,
        writeResult.deleteFiles().size(),
        matchedRows,
        table.name(),
        base.snapshotId(),
        plannedDataFiles,
        System.currentTimeMillis() - start);

    return new Result(
        base.snapshotId(),
        writeResult.deleteFiles(),
        writeResult.rewrittenDeleteFiles(),
        conflictFilter,
        plannedDataFiles);
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
    private final Long baseSnapshotId;
    private final List<DeleteFile> dvFiles;
    private final List<DeleteFile> rewrittenDvFiles;
    private final Expression conflictFilter;
    private final int plannedDataFiles;

    Result(
        Long baseSnapshotId,
        List<DeleteFile> dvFiles,
        List<DeleteFile> rewrittenDvFiles,
        Expression conflictFilter,
        int plannedDataFiles) {
      this.baseSnapshotId = baseSnapshotId;
      this.dvFiles = ImmutableList.copyOf(dvFiles);
      this.rewrittenDvFiles = ImmutableList.copyOf(rewrittenDvFiles);
      this.conflictFilter = conflictFilter;
      this.plannedDataFiles = plannedDataFiles;
    }

    /**
     * Snapshot the positions were resolved against, or null when there was no snapshot to resolve
     * against. A result without a snapshot has no deletion vectors, but the commit must still
     * validate that no data files matching {@link #conflictFilter()} were added, without a starting
     * snapshot, so that rows another writer commits to the branch first are detected.
     */
    public Long baseSnapshotId() {
      return baseSnapshotId;
    }

    /**
     * Deletion vectors to add. The data files they reference must still exist when the vectors are
     * committed, so the commit should pass their referenced data files to {@link
     * org.apache.iceberg.RowDelta#validateDataFilesExist(Iterable)}.
     */
    public List<DeleteFile> dvFiles() {
      return dvFiles;
    }

    /** Deletion vectors that were merged into a new one and must be removed. */
    public List<DeleteFile> rewrittenDvFiles() {
      return rewrittenDvFiles;
    }

    /**
     * Row filter covering every deleted key, for detecting data or delete files that were added
     * concurrently for those keys. It covers the keys also when {@link #baseSnapshotId()} is null.
     */
    public Expression conflictFilter() {
      return conflictFilter;
    }

    int plannedDataFiles() {
      return plannedDataFiles;
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
