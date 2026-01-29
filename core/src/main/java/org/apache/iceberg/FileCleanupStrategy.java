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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
  protected static final String MANIFEST = "manifest";
  protected static final String MANIFEST_LIST = "manifest list";
  protected static final String STATISTICS_FILES = "statistics files";

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
      ManifestFile.schema()
          .select(
              "manifest_path",
              "manifest_length",
              "partition_spec_id",
              "added_snapshot_id",
              "deleted_data_files_count");

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

  protected void deleteFiles(Set<String> pathsToDelete, String fileType, DeleteSummary summary) {
    if (deleteFunc == null && fileIO instanceof SupportsBulkOperations) {
      int failures = 0;
      try {
        ((SupportsBulkOperations) fileIO).deleteFiles(pathsToDelete);
      } catch (BulkDeletionFailureException e) {
        LOG.warn(
            "Bulk deletion failed for {} of {} {} file(s)",
            e.numberFailedObjects(),
            pathsToDelete.size(),
            fileType,
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
              (file, thrown) -> LOG.warn("Delete failed for {} file: {}", fileType, file, thrown))
          .run(
              file -> {
                deleteFuncToUse.accept(file);
                summary.deletedFile(fileType);
              });
    }
  }

  static class DeleteSummary {
    private final AtomicLong dataFilesCount = new AtomicLong(0L);
    private final AtomicLong positionDeleteFilesCount = new AtomicLong(0L);
    private final AtomicLong equalityDeleteFilesCount = new AtomicLong(0L);
    private final AtomicLong manifestsCount = new AtomicLong(0L);
    private final AtomicLong manifestListsCount = new AtomicLong(0L);
    private final AtomicLong statisticsFilesCount = new AtomicLong(0L);

    public void deletedFiles(String type, int numFiles) {
      if (FileContent.DATA.name().equalsIgnoreCase(type)) {
        dataFilesCount.addAndGet(numFiles);

      } else if (FileContent.POSITION_DELETES.name().equalsIgnoreCase(type)) {
        positionDeleteFilesCount.addAndGet(numFiles);

      } else if (FileContent.EQUALITY_DELETES.name().equalsIgnoreCase(type)) {
        equalityDeleteFilesCount.addAndGet(numFiles);

      } else if (MANIFEST.equalsIgnoreCase(type)) {
        manifestsCount.addAndGet(numFiles);

      } else if (MANIFEST_LIST.equalsIgnoreCase(type)) {
        manifestListsCount.addAndGet(numFiles);

      } else if (STATISTICS_FILES.equalsIgnoreCase(type)) {
        statisticsFilesCount.addAndGet(numFiles);

      } else {
        throw new ValidationException("Illegal file type: %s", type);
      }
    }

    public void deletedFile(String type) {
      if (FileContent.DATA.name().equalsIgnoreCase(type)) {
        dataFilesCount.incrementAndGet();

      } else if (FileContent.POSITION_DELETES.name().equalsIgnoreCase(type)) {
        positionDeleteFilesCount.incrementAndGet();

      } else if (FileContent.EQUALITY_DELETES.name().equalsIgnoreCase(type)) {
        equalityDeleteFilesCount.incrementAndGet();

      } else if (MANIFEST.equalsIgnoreCase(type)) {
        manifestsCount.incrementAndGet();

      } else if (MANIFEST_LIST.equalsIgnoreCase(type)) {
        manifestListsCount.incrementAndGet();

      } else if (STATISTICS_FILES.equalsIgnoreCase(type)) {
        statisticsFilesCount.incrementAndGet();

      } else {
        throw new ValidationException("Illegal file type: %s", type);
      }
    }

    public long dataFilesCount() {
      return dataFilesCount.get();
    }

    public long positionDeleteFilesCount() {
      return positionDeleteFilesCount.get();
    }

    public long equalityDeleteFilesCount() {
      return equalityDeleteFilesCount.get();
    }

    public long manifestsCount() {
      return manifestsCount.get();
    }

    public long manifestListsCount() {
      return manifestListsCount.get();
    }

    public long statisticsFilesCount() {
      return statisticsFilesCount.get();
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

  protected static class FileInfo {
    private final FileContent content;
    private final String path;

    public FileInfo(FileContent content, String path) {
      this.content = content;
      this.path = path;
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

      FileInfo fileInfo = (FileInfo) other;
      return Objects.equals(content, fileInfo.content) && Objects.equals(path, fileInfo.path);
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
