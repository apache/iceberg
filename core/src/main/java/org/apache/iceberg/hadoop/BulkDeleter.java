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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains ALL references to the hadoop bulk delete API; It will not be available on older hadoop
 * (pre 3.4.0) runtimes.
 */
final class BulkDeleter {

  private static final Logger LOG = LoggerFactory.getLogger(BulkDeleter.class);
  static final String BULK_DELETE_CLASS = "org/apache/hadoop/fs/BulkDelete.class";
  private static String resourceToScanFor = BULK_DELETE_CLASS;
  private final ExecutorService executorService;
  private final Configuration conf;

  /** */
  BulkDeleter(ExecutorService executorService, Configuration conf) {
    this.executorService = executorService;
    this.conf = conf;
  }

  /**
   * Is the bulk delete API available?
   *
   * @return true if the bulk delete interface class is on the classpath.
   */
  public static boolean apiAvailable() {
    return BulkDeleter.class.getClassLoader().getResource(resourceToScanFor) != null;
  }

  /**
   * Force set the name of the API resource to scan for in {@link #apiAvailable()}. This is purlely
   * to allow tests to inject failures by requesting a nonexistent resource.
   *
   * @param resource name of the resource to look for.
   */
  @VisibleForTesting
  static void setApiResource(String resource) {
    resourceToScanFor = resource;
  }

  /**
   * Bulk delete files.
   *
   * <p>When implemented in the hadoop filesystem APIs, all filesystems support a bulk delete of a
   * page size of 1 or more. On S3 a larger bulk delete operation is supported, with the page size
   * set by {@code fs.s3a.bulk.delete.page.size}. Note: some third party S3 stores do not support
   * bulk delete, in which case the page size is 1).
   *
   * <p>A page of paths to delete is built up for each filesystem; when the page size is reached a
   * bulk delete is submitted for execution in a separate thread.
   *
   * @param pathnames paths to delete.
   * @return count of failures.
   * @throws UncheckedIOException if an IOE was raised in the invoked methods.
   * @throws RuntimeException if interrupted while waiting for deletions to complete.
   */
  public int bulkDeleteFiles(Iterable<String> pathnames) {

    LOG.debug("Using bulk delete operation to delete files");

    // Bulk deletion for each filesystem in the path names
    Map<Path, DeleteContext> deletionMap = Maps.newHashMap();

    // deletion tasks submitted.
    List<Future<List<Map.Entry<Path, String>>>> deletionTasks = Lists.newArrayList();

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
        // build root path of the filesystem,
        Path fsRoot = fs.makeQualified(new Path("/"));
        DeleteContext deleteContext = deletionMap.get(fsRoot);
        if (deleteContext == null) {
          // fs root is not in the map, so create the bulk delete operation for
          // for that FS and store within a new delete context.
          deleteContext = new DeleteContext(fs.createBulkDelete(fsRoot));
          deletionMap.put(fsRoot, deleteContext);
        }

        // add the deletion target.
        deleteContext.add(target);

        if (deleteContext.pageIsComplete()) {
          // the page size has been reached.

          // get the live path list, which MUST be done outside the async
          // submitted closure.
          final Collection<Path> paths = deleteContext.snapshotDeletedFiles();
          final BulkDelete bulkDeleter = deleteContext.bulkDeleter();
          // execute the bulk delete in a new thread then prepare the delete path set for new
          // entries.
          deletionTasks.add(executorService.submit(() -> deleteBatch(bulkDeleter, paths)));
        }
      }

      // End of the iteration. Submit deletion batches for all
      // entries in the map which haven't yet reached their page size
      deletionMap.values().stream()
          .filter(sd -> sd.size() > 0)
          .map(sd -> executorService.submit(() -> deleteBatch(sd.bulkDeleter(), sd.deletedFiles)))
          .forEach(deletionTasks::add);

      // Wait for all deletion tasks to complete and count the failures.
      LOG.debug("Waiting for {} deletion tasks to complete", deletionTasks.size());

      for (Future<List<Map.Entry<Path, String>>> deletionTask : deletionTasks) {
        try {
          List<Map.Entry<Path, String>> failedDeletions = deletionTask.get();
          failedDeletions.forEach(
              entry ->
                  LOG.warn(
                      "Failed to delete object at path {}: {}", entry.getKey(), entry.getValue()));
          totalFailedDeletions += failedDeletions.size();
        } catch (ExecutionException e) {
          LOG.warn("Caught unexpected exception during batch deletion: ", e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          deletionTasks.stream().filter(task -> !task.isDone()).forEach(task -> task.cancel(true));
          throw new RuntimeException("Interrupted when waiting for deletions to complete", e);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      deletionMap.values().forEach(DeleteContext::close);
    }

    return totalFailedDeletions;
  }

  /**
   * Delete a single batch of paths.
   *
   * @param bulkDeleter deleter
   * @param paths paths to delete.
   * @return the list of paths which couldn't be deleted.
   * @throws UncheckedIOException if an IOE was raised in the invoked methods.
   */
  private List<Map.Entry<Path, String>> deleteBatch(
      BulkDelete bulkDeleter, Collection<Path> paths) {

    LOG.debug("Deleting batch of {} paths", paths.size());
    try {
      return bulkDeleter.bulkDelete(paths);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Ongoing store deletion; built up i */
  private static final class DeleteContext implements AutoCloseable {
    private final BulkDelete bulkDelete;
    private final int pageSize;
    private Set<Path> deletedFiles;

    private DeleteContext(BulkDelete bulkDelete) {
      this.bulkDelete = bulkDelete;
      this.pageSize = bulkDelete.pageSize();
    }

    /**
     * This is a very quiet close, for use in cleanup.Up to Hadoop 3.5.0 the bulk delete objects
     * always have no-op close calls. This is due diligence.
     */
    @Override
    public void close() {
      try {
        bulkDelete.close();
      } catch (IOException ignored) {
        LOG.debug("Failed to close bulk delete", ignored);
      }
    }

    private void add(Path path) {
      if (deletedFiles == null) {
        deletedFiles = Sets.newHashSet();
      }
      deletedFiles.add(path);
    }

    public BulkDelete bulkDeleter() {
      return bulkDelete;
    }

    private int size() {
      return deletedFiles == null ? 0 : deletedFiles.size();
    }

    public int pageSize() {
      return pageSize;
    }

    public boolean pageIsComplete() {
      return size() == pageSize();
    }

    private Set<Path> snapshotDeletedFiles() {
      final Set<Path> paths = deletedFiles == null ? Collections.emptySet() : deletedFiles;
      deletedFiles = null;
      return paths;
    }
  }
}
