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

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
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
  public abstract void cleanFiles(
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

  protected void deleteFiles(Set<String> pathsToDelete, String fileType) {
    if (deleteFunc == null && fileIO instanceof SupportsBulkOperations) {
      try {
        ((SupportsBulkOperations) fileIO).deleteFiles(pathsToDelete);
      } catch (BulkDeletionFailureException e) {
        LOG.warn(
            "Bulk deletion failed for {} of {} {} file(s)",
            e.numberFailedObjects(),
            pathsToDelete.size(),
            fileType,
            e);
      } catch (RuntimeException e) {
        LOG.warn("Bulk deletion failed", e);
      }
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
          .run(deleteFuncToUse::accept);
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
