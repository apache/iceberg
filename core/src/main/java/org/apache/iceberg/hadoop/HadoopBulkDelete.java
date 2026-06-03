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
package org.apache.iceberg.hadoop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains references to the hadoop bulk delete API; It will not be available on hadoop 3.3.x
 * runtimes.
 */
final class HadoopBulkDelete {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopBulkDelete.class);

  /** Resource looked for as an availability probe: {@value}. */
  private static final String BULK_DELETE_CLASS = "org/apache/hadoop/fs/BulkDelete.class";

  /** Thread pool for deletions. */
  private final ExecutorService executorService;

  /** Configuration for filesystems to retrieve. */
  private final Configuration conf;

  /**
   * Test method to mark as unavailable without having to play games with static method mocking that
   * don't work reliably.
   */
  private static boolean unavailable = false;

  /**
   * Constructor.
   *
   * @param executorService pool for executing bulk delete in parallel
   * @param conf hadoop configuration used to instantiate filesystems.
   */
  HadoopBulkDelete(ExecutorService executorService, Configuration conf) {
    this.executorService = executorService;
    this.conf = conf;
  }

  /**
   * Declare the API is unavailable or restore it.
   *
   * @param flag unavailability flag: true if the API is not found.
   */
  @VisibleForTesting
  static void markApiUnavailable(boolean flag) {
    unavailable = flag;
  }

  /**
   * Is the bulk delete API available?
   *
   * @return true if the bulk delete interface class is on the classpath.
   */
  static boolean apiAvailable() {
    return !unavailable
        && HadoopBulkDelete.class.getClassLoader().getResource(BULK_DELETE_CLASS) != null;
  }

  /**
   * Execute {@code SupportsBulkOperations.deleteFiles(Iterable<String>)}, given a thread pool and a
   * configuration to instantiate filesystems with.
   *
   * @param pathsToDelete paths to be deleted
   * @param executorService executor service
   * @param conf configuration.
   * @throws BulkDeletionFailureException failure to delete one or more files.
   * @throws IllegalStateException if the hadoop runtime does not support bulk delete.
   */
  static void deleteFiles(
      final Iterable<String> pathsToDelete,
      final ExecutorService executorService,
      final Configuration conf) {
    final boolean available = apiAvailable();
    Preconditions.checkState(
        available,
        "Bulk delete has been enabled but is not present within the current hadoop library. "
            + "Review the value of "
            + HadoopFileIO.BULK_DELETE_ENABLED);
    final int count = new HadoopBulkDelete(executorService, conf).bulkDeleteFiles(pathsToDelete);
    if (count != 0) {
      throw new BulkDeletionFailureException(count);
    }
  }

  /**
   * Bulk delete files.
   *
   * <p>When implemented in the hadoop filesystem APIs, all filesystems support a bulk delete of a
   * page size of at least one. On S3A a larger bulk delete operation is supported, with the page
   * size set by {@code fs.s3a.bulk.delete.page.size}.
   *
   * <p>A page of paths to delete is built up for each filesystem; when the page size is reached a
   * bulk delete is submitted for execution in a separate thread.
   *
   * @param pathnames paths to delete.
   * @return count of failures.
   * @throws UncheckedIOException if an IOE was raised in the invoked methods.
   * @throws RuntimeException if interrupted while waiting for deletions to complete.
   */
  int bulkDeleteFiles(Iterable<String> pathnames) {

    LOG.debug("Using bulk delete operation to delete files");

    // Deletion context for each filesystem, using the root path as lookup.
    Map<Path, DeleteContext> contextMap = Maps.newHashMap();

    // List of ongoing deletion tasks.
    List<Future<Outcome>> deletionTasks = Lists.newArrayList();

    int totalFailedDeletions = 0;

    try {
      for (String name : pathnames) {
        Path target = new Path(name);
        final FileSystem fs;
        try {
          fs = target.getFileSystem(conf);
        } catch (Exception e) {
          // any failure to find/load a filesystem
          LOG.info("Failed to get filesystem for path: {}; unable to delete it", target, e);
          totalFailedDeletions++;
          continue;
        }
        // a delete context is built for each fs, mapped by the root path of that filesystem.
        Path fsRoot = fs.makeQualified(new Path("/"));
        DeleteContext dc = contextMap.get(fsRoot);
        if (dc == null) {
          // the fs root path is not in the map, so create the bulk delete operation for
          // that FS and store within a new delete context.
          dc = new DeleteContext(fsRoot, fs.createBulkDelete(fsRoot));
          contextMap.put(fsRoot, dc);
        }

        // make final for the closure use.
        final DeleteContext deleteContext = dc;

        // add the deletion target.
        deleteContext.add(target);

        if (deleteContext.pageSizeReached()) {
          // the page size has been reached.
          // execute the bulk delete in a new thread.
          deletionTasks.add(executorService.submit(deleteContext.createDeletionTask()));
        }
      }

      // End of the iteration. Submit deletion batches for all
      // entries in the map which haven't yet reached their page size
      contextMap.values().stream()
          .filter(sd -> !sd.isEmpty())
          .map(sd -> executorService.submit(sd.finalDeletionTask()))
          .forEach(deletionTasks::add);

      // Wait for all deletion tasks to complete and report any failures.
      LOG.debug("Waiting for {} deletion tasks to complete", deletionTasks.size());

      for (Future<Outcome> deletionTask : deletionTasks) {
        try {
          List<DeleteFailure> failedDeletions = deletionTask.get(1, TimeUnit.HOURS).failures();
          failedDeletions.forEach(
              entry ->
                  LOG.warn(
                      "Failed to delete object at path {}: {}", entry.path(), entry.errorText()));
          totalFailedDeletions += failedDeletions.size();
        } catch (ExecutionException e) {
          LOG.warn("Caught unexpected exception during batch deletion: ", e.getCause());
        } catch (InterruptedException | TimeoutException e) {
          Thread.currentThread().interrupt();
          deletionTasks.stream().filter(task -> !task.isDone()).forEach(task -> task.cancel(true));
          throw new RuntimeException("Interrupted when waiting for deletions to complete", e);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      contextMap.values().forEach(DeleteContext::close);
    }
    return totalFailedDeletions;
  }

  /**
   * Delete context for a single filesystem. Tracks files to delete, the callback to invoke, knows
   * when the page size is reached and is how bulkDelete() is finally invoked.
   */
  @VisibleForTesting
  static final class DeleteContext implements AutoCloseable {
    /** root of this delete context. */
    private final Path fsRoot;

    /** Hadoop bulk delete operation for a filesystem. */
    private final BulkDelete bulkDelete;

    /** page size. */
    private final int pageSize;

    /** set of deleted files; demand created. */
    private Set<Path> deletedFiles;

    /**
     * Bind to a bulk delete instance. Acquires and stores the page size from it.
     *
     * @param bulkDelete bulk delete operation.
     */
    DeleteContext(Path fsRoot, BulkDelete bulkDelete) {
      this.fsRoot = Preconditions.checkNotNull(fsRoot, "null filesystem root path");
      this.bulkDelete = Preconditions.checkNotNull(bulkDelete, "null filesystem bulk deleter");
      this.pageSize = bulkDelete.pageSize();
      Preconditions.checkArgument(pageSize > 0, "Page size must be greater than zero");
    }

    /**
     * Return a callable which will delete the current set of paths to delete. After returning new
     * paths added to the context will accrue for later tasks.
     *
     * @return a callable.
     */
    Callable<Outcome> createDeletionTask() {
      // get the live path list, which MUST be done outside the async
      // submitted closure. This also resets the context list to prepare
      // for more entries.
      final Collection<Path> paths = snapshotDeletedFiles();
      return () -> deleteBatch(paths);
    }

    /**
     * The final deletion task, which doesn't snapshot the current list of deleted paths.
     *
     * @return a callable to execute.
     */
    Callable<Outcome> finalDeletionTask() {
      return () -> deleteBatch(deletedFiles());
    }

    /** This is a very quiet close, for use in cleanup. This is due diligence. */
    @Override
    public void close() {
      try {
        bulkDelete.close();
      } catch (IOException e) {
        LOG.debug("Failed to close bulk delete", e);
      }
    }

    /**
     * Add a path, creating the path set on demand.
     *
     * @param path path to add.
     */
    void add(Path path) {
      if (deletedFiles == null) {
        deletedFiles = Sets.newHashSet();
      }
      deletedFiles.add(path);
      Preconditions.checkState(
          deletedFiles.size() <= pageSize, "Number of queued items to delete exceeds page size");
    }

    Path fsRoot() {
      return fsRoot;
    }

    BulkDelete bulkDelete() {
      return bulkDelete;
    }

    /**
     * Live list of deleted files.
     *
     * @return the ongoing list being built up.
     */
    Set<Path> deletedFiles() {
      return deletedFiles;
    }

    /**
     * Number of files to queued to delete.
     *
     * @return current number of files queued to delete.
     */
    int size() {
      return deletedFiles == null ? 0 : deletedFiles.size();
    }

    /**
     * Cached page size of the BulkDelete instance.
     *
     * @return a positive integer.
     */
    int pageSize() {
      return pageSize;
    }

    /**
     * Has the page size been reached?
     *
     * @return true if the set of deleted files matches the page size.
     */
    boolean pageSizeReached() {
      return size() == pageSize();
    }

    /**
     * Is the queue empty?
     *
     * @return true if there are no files to delete.
     */
    boolean isEmpty() {
      return deletedFiles == null || deletedFiles.isEmpty();
    }

    /**
     * Take a snapshot of the deleted files for passing to an asynchronous deletion operation. The
     * {@link #deletedFiles} field is reset.
     *
     * @return the set of unique filenames passed in for deletion.
     */
    Set<Path> snapshotDeletedFiles() {
      final Set<Path> paths = deletedFiles == null ? Collections.emptySet() : deletedFiles;
      deletedFiles = null;
      return paths;
    }

    /**
     * Delete a single batch of paths.
     *
     * @param paths paths to delete.
     * @return An outcome containing the list of paths which couldn't be deleted.
     * @throws UncheckedIOException if an IOE was raised in the invoked methods.
     */
    Outcome deleteBatch(Collection<Path> paths) {
      LOG.debug("Deleting batch of {} paths", paths.size());
      try {
        return new Outcome(
            bulkDelete().bulkDelete(paths).stream()
                .map(e -> new DeleteFailure(e.getKey(), e.getValue()))
                .toList());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /** A failure to delete a single path. */
  @VisibleForTesting
  static class DeleteFailure {
    private final Path path;
    private final String errorText;

    /**
     * Delete failure.
     *
     * @param path path which couldn't be deleted
     * @param errorText error text
     */
    DeleteFailure(Path path, String errorText) {
      this.path = path;
      this.errorText = errorText;
    }

    String errorText() {
      return errorText;
    }

    Path path() {
      return path;
    }
  }

  /** The outcome of a bulk delete. */
  @VisibleForTesting
  static class Outcome {
    private final List<DeleteFailure> failures;

    /**
     * Record the outcome of a bulk delete.
     *
     * @param failures a list of failures and their error strings.
     */
    Outcome(final List<DeleteFailure> failures) {
      this.failures = failures;
    }

    List<DeleteFailure> failures() {
      return failures;
    }
  }
}
