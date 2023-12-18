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
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A utility for cleaning up written but not committed files. */
class SparkCleanupUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SparkCleanupUtil.class);

  private static final int DELETE_NUM_RETRIES = 3;
  private static final int DELETE_MIN_RETRY_WAIT_MS = 100; // 100 ms
  private static final int DELETE_MAX_RETRY_WAIT_MS = 30 * 1000; // 30 seconds
  private static final int DELETE_TOTAL_RETRY_TIME_MS = 2 * 60 * 1000; // 2 minutes

  private SparkCleanupUtil() {}

  /**
   * Attempts to delete as many files produced by a task as possible.
   *
   * <p>Note this method will log Spark task info and is supposed to be called only on executors.
   * Use {@link #deleteFiles(String, FileIO, List)} to delete files on the driver.
   *
   * @param io a {@link FileIO} instance used for deleting files
   * @param files a list of files to delete
   */
  public static void deleteTaskFiles(FileIO io, List<? extends ContentFile<?>> files) {
    deleteFiles(taskInfo(), io, files);
  }

  // the format matches what Spark uses for internal logging
  private static String taskInfo() {
    TaskContext taskContext = TaskContext.get();
    if (taskContext == null) {
      return "unknown task";
    } else {
      return String.format(
          "partition %d (task %d, attempt %d, stage %d.%d)",
          taskContext.partitionId(),
          taskContext.taskAttemptId(),
          taskContext.attemptNumber(),
          taskContext.stageId(),
          taskContext.stageAttemptNumber());
    }
  }

  /**
   * Attempts to delete as many given files as possible.
   *
   * @param context a helpful description of the operation invoking this method
   * @param io a {@link FileIO} instance used for deleting files
   * @param files a list of files to delete
   */
  public static void deleteFiles(String context, FileIO io, List<? extends ContentFile<?>> files) {
    List<String> paths = Lists.transform(files, file -> file.path().toString());
    deletePaths(context, io, paths);
  }

  private static void deletePaths(String context, FileIO io, List<String> paths) {
    if (io instanceof SupportsBulkOperations) {
      SupportsBulkOperations bulkIO = (SupportsBulkOperations) io;
      bulkDelete(context, bulkIO, paths);
    } else {
      delete(context, io, paths);
    }
  }

  private static void bulkDelete(String context, SupportsBulkOperations io, List<String> paths) {
    try {
      io.deleteFiles(paths);
      LOG.info("Deleted {} file(s) using bulk deletes ({})", paths.size(), context);

    } catch (BulkDeletionFailureException e) {
      int deletedFilesCount = paths.size() - e.numberFailedObjects();
      LOG.warn(
          "Deleted only {} of {} file(s) using bulk deletes ({})",
          deletedFilesCount,
          paths.size(),
          context);
    }
  }

  private static void delete(String context, FileIO io, List<String> paths) {
    AtomicInteger deletedFilesCount = new AtomicInteger(0);

    Tasks.foreach(paths)
        .executeWith(ThreadPools.getWorkerPool())
        .stopRetryOn(NotFoundException.class)
        .suppressFailureWhenFinished()
        .onFailure((path, exc) -> LOG.warn("Failed to delete {} ({})", path, context, exc))
        .retry(DELETE_NUM_RETRIES)
        .exponentialBackoff(
            DELETE_MIN_RETRY_WAIT_MS,
            DELETE_MAX_RETRY_WAIT_MS,
            DELETE_TOTAL_RETRY_TIME_MS,
            2 /* exponential */)
        .run(
            path -> {
              io.deleteFile(path);
              deletedFilesCount.incrementAndGet();
            });

    if (deletedFilesCount.get() < paths.size()) {
      LOG.warn("Deleted only {} of {} file(s) ({})", deletedFilesCount, paths.size(), context);
    } else {
      LOG.info("Deleted {} file(s) ({})", paths.size(), context);
    }
  }
}
