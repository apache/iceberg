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
package org.apache.iceberg;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:VisibilityModifier")
abstract class FileCleanupStrategy {
  private final Consumer<String> defaultDeleteFunc =
      new Consumer<>() {
        @Override
        public void accept(String file) {
          fileIO.deleteFile(file);
        }
      };

  private static final Logger LOG = LoggerFactory.getLogger(FileCleanupStrategy.class);

  protected final FileIO fileIO;
  protected final ExecutorService planExecutorService;
  private final Consumer<String> deleteFunc;
  private final ExecutorService deleteExecutorService;

  protected FileCleanupStrategy(
      FileIO fileIO,
      ExecutorService deleteExecutorService,
      ExecutorService planExecutorService,
      Consumer<String> deleteFunc) {
    this.fileIO = fileIO;
    this.deleteExecutorService = deleteExecutorService;
    this.planExecutorService = planExecutorService;
    this.deleteFunc = deleteFunc;
  }

  /**
   * Clean up files that are only reachable by expired snapshots.
   *
   * <p>This method is responsible for identifying and deleting files that are safe to remove based
   * on the table metadata state before and after snapshot expiration. The cleanup level controls
   * which types of files are eligible for deletion.
   *
   * <p>Note that {@link ExpireSnapshots.CleanupLevel#NONE} is handled before reaching this method
   *
   * @param beforeExpiration table metadata before snapshot expiration
   * @param afterExpiration table metadata after snapshot expiration
   * @param cleanupLevel controls which types of files are eligible for deletion
   */
  public abstract DeleteSummary cleanFiles(
      TableMetadata beforeExpiration,
      TableMetadata afterExpiration,
      ExpireSnapshots.CleanupLevel cleanupLevel);

  private static final Schema MANIFEST_PROJECTION =
      TypeUtil.select(
          ManifestFile.schema(),
          ImmutableSet.of(
              ManifestFile.PATH.fieldId(),
              ManifestFile.LENGTH.fieldId(),
              ManifestFile.SPEC_ID.fieldId(),
              ManifestFile.SNAPSHOT_ID.fieldId(),
              ManifestFile.ADDED_FILES_COUNT.fieldId(),
              ManifestFile.DELETED_FILES_COUNT.fieldId()));

  protected CloseableIterable<ManifestFile> readManifests(Snapshot snapshot) {
    if (snapshot.manifestListLocation() != null) {
      return InternalData.read(
              FileFormat.AVRO, fileIO.newInputFile(snapshot.manifestListLocation()))
          .setRootType(GenericManifestFile.class)
          .project(MANIFEST_PROJECTION)
          .reuseContainers()
          .build();
    } else {
      return CloseableIterable.withNoopClose(snapshot.allManifests(fileIO));
    }
  }

  protected void deleteFiles(
      Set<String> pathsToDelete, DeletedFileType fileType, DeleteSummary summary) {
    if (deleteFunc == null && fileIO instanceof SupportsBulkOperations) {
      int failures = 0;
      try {
        ((SupportsBulkOperations) fileIO).deleteFiles(pathsToDelete);
      } catch (BulkDeletionFailureException e) {
        LOG.warn(
            "Bulk deletion failed for {} of {} {} file(s)",
            e.numberFailedObjects(),
            pathsToDelete.size(),
            fileType.displayName(),
            e);
        failures = e.numberFailedObjects();
      } catch (RuntimeException e) {
        LOG.warn("Bulk deletion failed", e);
      }
      summary.deletedFiles(fileType, pathsToDelete.size() - failures);
    } else {
      Consumer<String> deleteFuncToUse = deleteFunc == null ? defaultDeleteFunc : deleteFunc;

      Tasks.foreach(pathsToDelete)
          .executeWith(deleteExecutorService)
          .retry(3)
          .stopRetryOn(NotFoundException.class)
          .stopOnFailure()
          .suppressFailureWhenFinished()
          .onFailure(
              (file, thrown) ->
                  LOG.warn("Delete failed for {} file: {}", fileType.displayName(), file, thrown))
          .run(
              file -> {
                deleteFuncToUse.accept(file);
                summary.deletedFile(fileType);
              });
    }
  }

  enum DeletedFileType {
    DATA("data"),
    POSITION_DELETES("position delete"),
    EQUALITY_DELETES("equality delete"),
    MANIFEST("manifest"),
    MANIFEST_LIST("manifest list"),
    STATISTICS_FILES("statistics files");

    private final String displayName;

    DeletedFileType(String displayName) {
      this.displayName = displayName;
    }

    public String displayName() {
      return displayName;
    }

    public static DeletedFileType fromContent(FileContent content) {
      switch (content) {
        case DATA:
          return DATA;
        case POSITION_DELETES:
          return POSITION_DELETES;
        case EQUALITY_DELETES:
          return EQUALITY_DELETES;
        default:
          throw new ValidationException("Illegal file content: %s", content);
      }
    }
  }

  static class DeleteSummary {
    private final Map<DeletedFileType, AtomicLong> counts;

    DeleteSummary() {
      Map<DeletedFileType, AtomicLong> map = Maps.newEnumMap(DeletedFileType.class);
      for (DeletedFileType type : DeletedFileType.values()) {
        map.put(type, new AtomicLong(0L));
      }
      this.counts = map;
    }

    public void deletedFiles(DeletedFileType type, int numFiles) {
      counts.get(type).addAndGet(numFiles);
    }

    public void deletedFile(DeletedFileType type) {
      deletedFiles(type, 1);
    }

    public long dataFilesCount() {
      return counts.get(DeletedFileType.DATA).get();
    }

    public long positionDeleteFilesCount() {
      return counts.get(DeletedFileType.POSITION_DELETES).get();
    }

    public long equalityDeleteFilesCount() {
      return counts.get(DeletedFileType.EQUALITY_DELETES).get();
    }

    public long manifestsCount() {
      return counts.get(DeletedFileType.MANIFEST).get();
    }

    public long manifestListsCount() {
      return counts.get(DeletedFileType.MANIFEST_LIST).get();
    }

    public long statisticsFilesCount() {
      return counts.get(DeletedFileType.STATISTICS_FILES).get();
    }
  }

  protected boolean hasAnyStatisticsFiles(TableMetadata tableMetadata) {
    return !tableMetadata.statisticsFiles().isEmpty()
        || !tableMetadata.partitionStatisticsFiles().isEmpty();
  }

  protected Set<String> expiredStatisticsFilesLocations(
      TableMetadata beforeExpiration, TableMetadata afterExpiration) {
    Set<String> statsFileLocationsBeforeExpiration = statsFileLocations(beforeExpiration);
    Set<String> statsFileLocationsAfterExpiration = statsFileLocations(afterExpiration);

    return Sets.difference(statsFileLocationsBeforeExpiration, statsFileLocationsAfterExpiration);
  }

  protected static class ExpiredContentFile {
    private final FileContent content;
    private final String path;

    public ExpiredContentFile(FileContent content, String path) {
      this.content = Preconditions.checkNotNull(content, "content is null");
      this.path = Preconditions.checkNotNull(path, "path is null");
    }

    public FileContent getContent() {
      return content;
    }

    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other == null || getClass() != other.getClass()) {
        return false;
      }

      ExpiredContentFile that = (ExpiredContentFile) other;
      return Objects.equals(content, that.content) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(content, path);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("content", content).add("path", path).toString();
    }
  }

  private Set<String> statsFileLocations(TableMetadata tableMetadata) {
    Set<String> statsFileLocations = Sets.newHashSet();

    if (tableMetadata.statisticsFiles() != null) {
      tableMetadata.statisticsFiles().stream()
          .map(StatisticsFile::path)
          .forEach(statsFileLocations::add);
    }

    if (tableMetadata.partitionStatisticsFiles() != null) {
      tableMetadata.partitionStatisticsFiles().stream()
          .map(PartitionStatisticsFile::path)
          .forEach(statsFileLocations::add);
    }

    return statsFileLocations;
  }
}
