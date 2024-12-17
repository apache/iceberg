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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.wrappedio.DynamicWrappedIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.SetMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopFileIO implements HadoopConfigurable, DelegateFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileIO.class);
  private static final String DELETE_FILE_PARALLELISM = "iceberg.hadoop.delete-file-parallelism";
  /** Is bulk delete enabled on hadoop runtimes with API support: {@value}. */
  public static final String BULK_DELETE_ENABLED = "iceberg.hadoop.bulk.delete.enabled";

  private static final boolean BULK_DELETE_ENABLED_DEFAULT = true;
  private static final String DELETE_FILE_POOL_NAME = "iceberg-hadoopfileio-delete";
  private static final int DELETE_RETRY_ATTEMPTS = 3;
  private static final int DEFAULT_DELETE_CORE_MULTIPLE = 4;
  private static volatile ExecutorService executorService;

  private SerializableSupplier<Configuration> hadoopConf;
  private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

  /** Has an attempt to configure the bulk delete binding been made? */
  private final AtomicBoolean bulkDeleteConfigured = new AtomicBoolean(false);

  /**
   * Dynamically loaded accessor of Hadoop Wrapped IO classes. Marked as volatile as its creation in
   * {@link #maybeUseBulkDeleteApi()} is synchronized and IDEs then complain about mixed use.
   */
  private transient volatile DynamicWrappedIO wrappedIO;

  /** Flag to indicate that bulk delete is present and should be used. */
  private boolean useBulkDelete;

  /**
   * Constructor used for dynamic FileIO loading.
   *
   * <p>{@link Configuration Hadoop configuration} must be set through {@link
   * HadoopFileIO#setConf(Configuration)}
   */
  public HadoopFileIO() {}

  public HadoopFileIO(Configuration hadoopConf) {
    this(new SerializableConfiguration(hadoopConf)::get);
  }

  public HadoopFileIO(SerializableSupplier<Configuration> hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  public Configuration conf() {
    return getConf();
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
  }

  @Override
  public InputFile newInputFile(String path) {
    return HadoopInputFile.fromLocation(path, getConf());
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return HadoopInputFile.fromLocation(path, length, getConf());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return HadoopOutputFile.fromPath(new Path(path), getConf());
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFs(toDelete, getConf());
    try {
      fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = new SerializableConfiguration(conf)::get;
  }

  @Override
  public Configuration getConf() {
    // Create a default hadoopConf as it is required for the object to be valid.
    // E.g. newInputFile would throw NPE with getConf() otherwise.
    if (hadoopConf == null) {
      synchronized (this) {
        if (hadoopConf == null) {
          this.hadoopConf = new SerializableConfiguration(new Configuration())::get;
        }
      }
    }

    return hadoopConf.get();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    this.hadoopConf = confSerializer.apply(getConf());
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    Path prefixToList = new Path(prefix);
    FileSystem fs = Util.getFs(prefixToList, getConf());

    return () -> {
      try {
        return Streams.stream(
                new AdaptingIterator<>(fs.listFiles(prefixToList, true /* recursive */)))
            .map(
                fileStatus ->
                    new FileInfo(
                        fileStatus.getPath().toString(),
                        fileStatus.getLen(),
                        fileStatus.getModificationTime()))
            .iterator();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  @Override
  public void deletePrefix(String prefix) {
    Path prefixToDelete = new Path(prefix);
    FileSystem fs = Util.getFs(prefixToDelete, getConf());

    try {
      fs.delete(prefixToDelete, true /* recursive */);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Initialize the wrapped IO class if configured to do so.
   *
   * @return true if bulk delete should be used.
   */
  private synchronized boolean maybeUseBulkDeleteApi() {
    if (!bulkDeleteConfigured.compareAndSet(false, true)) {
      // configured already, so return.
      return useBulkDelete;
    }
    boolean enableBulkDelete = conf().getBoolean(BULK_DELETE_ENABLED, BULK_DELETE_ENABLED_DEFAULT);
    if (!enableBulkDelete) {
      LOG.debug("Bulk delete is disabled");
      useBulkDelete = false;
    } else {
      // library is configured to use bulk delete, so try to load it
      // and probe for the bulk delete methods being found.
      // this is only satisfied on Hadoop releases with the WrappedIO class.
      wrappedIO = new DynamicWrappedIO(this.getClass().getClassLoader());
      useBulkDelete = wrappedIO.bulkDeleteAvailable();
      if (useBulkDelete) {
        LOG.debug("Bulk delete is enabled and available");
      } else {
        LOG.debug("Bulk delete enabled but not available");
      }
    }
    return useBulkDelete;
  }

  /**
   * Will this instance use bulk delete operations? Exported for testing. Will trigger a load of the
   * WrappedIO class if required.
   *
   * @return true if bulk delete is enabled.
   */
  @VisibleForTesting
  boolean isBulkDeleteApiUsed() {
    return maybeUseBulkDeleteApi();
  }

  /**
   * Delete files.
   *
   * <p>If the Hadoop bulk deletion API is available and enabled, this API is used through {@link
   * #bulkDeleteFiles(Iterable)}. Otherwise, each file is deleted individually in the thread pool.
   *
   * @param pathsToDelete The paths to delete
   * @throws BulkDeletionFailureException failure to delete one or more files.
   */
  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    AtomicInteger failureCount = new AtomicInteger(0);
    if (maybeUseBulkDeleteApi()) {
      // bulk delete.
      failureCount.set(bulkDeleteFiles(pathsToDelete));
    } else {
      // classic delete in which each file is deleted individually
      // in a separate thread.
      Tasks.foreach(pathsToDelete)
          .executeWith(executorService())
          .retry(DELETE_RETRY_ATTEMPTS)
          .stopRetryOn(FileNotFoundException.class)
          .suppressFailureWhenFinished()
          .onFailure(
              (f, e) -> {
                LOG.error("Failure during bulk delete on file: {} ", f, e);
                failureCount.incrementAndGet();
              })
          .run(this::deleteFile);
    }
    if (failureCount.get() != 0) {
      throw new BulkDeletionFailureException(failureCount.get());
    }
  }

  /**
   * Bulk delete files.
   *
   * <p>When implemented in the hadoop filesystem APIs, all filesystems support a bulk delete of a
   * page size &gt; 1. On S3 a larger bulk delete operation is supported, with the page size set by
   * {@code fs.s3a.bulk.delete.page.size}. Note: some third party S3 stores do not support bulk
   * delete, in which case the page size is 1).
   *
   * <p>A page of paths to delete is built up for each filesystem; when the page size is reached a
   * bulk delete is submitted for execution in a separate thread.
   *
   * <p>S3A Implementation Notes:
   *
   * <ol>
   *   <li>The default page size is 250 files; this is to handle throttling better.
   *   <li>The API can be rate limited through the option {@code fs.s3a.io.rate.limit}; each file
   *       uses "1" of the available write operations. Setting this option to a value greater than
   *       zero will reduce the risk of bulk deletion operations affecting the performance of other
   *       applications.
   * </ol>
   *
   * @param pathnames paths to delete.
   * @return count of failures.
   * @throws UncheckedIOException if an IOE was raised in the invoked methods.
   * @throws RuntimeException if interrupted while waiting for deletions to complete.
   */
  private int bulkDeleteFiles(Iterable<String> pathnames) {

    LOG.debug("Using bulk delete operation to delete files");

    // This has to support a list spanning multiple filesystems, so we group the paths by
    // the root path of each filesystem.
    SetMultimap<Path, Path> fsMap = Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);

    // this map of filesystem root to page size reduces the amount of
    // reflective invocations on the filesystems needed, and any work there.
    // this ensures that on scale tests with the default "page size == 1" bulk
    // delete implementation, execution time is no slower than the classic
    // delete implementation.
    Map<Path, Integer> fsPageSizeMap = Maps.newHashMap();

    // deletion tasks submitted.
    List<Future<List<Map.Entry<Path, String>>>> deletionTasks = Lists.newArrayList();

    final Path rootPath = new Path("/");
    final Configuration conf = hadoopConf.get();
    int totalFailedDeletions = 0;

    for (String name : pathnames) {
      Path target = new Path(name);
      final FileSystem fs;
      try {
        fs = Util.getFs(target, conf);
      } catch (Exception e) {
        // any failure to find/load a filesystem
        LOG.warn("Failed to get filesystem for path: {}", target, e);
        totalFailedDeletions++;
        continue;
      }
      // build root path of the filesystem.
      Path fsRoot = fs.makeQualified(rootPath);
      int pageSize;
      if (!fsPageSizeMap.containsKey(fsRoot)) {
        pageSize = wrappedIO.bulkDelete_pageSize(fs, rootPath);
        fsPageSizeMap.put(fsRoot, pageSize);
      } else {
        pageSize = fsPageSizeMap.get(fsRoot);
      }

      // retrieve or create set paths for the specific filesystem
      Set<Path> pathsForFilesystem = fsMap.get(fsRoot);
      // add the target. This updates the value in the map.
      pathsForFilesystem.add(target);

      if (pathsForFilesystem.size() == pageSize) {
        // the page size has been reached.
        // for classic filesystems page size == 1 so this happens every time.
        // hence: try and keep it efficient.

        // clone the live path list, which MUST be done outside the async
        // submitted closure.
        Collection<Path> paths = Sets.newHashSet(pathsForFilesystem);
        // submit the batch deletion task.
        deletionTasks.add(executorService().submit(() -> deleteBatch(fs, fsRoot, paths)));
        // remove all paths for this fs from the map.
        fsMap.removeAll(fsRoot);
      }
    }

    // End of the iteration. Submit deletion batches for all
    // entries in the map which haven't yet reached their page size
    for (Map.Entry<Path, Collection<Path>> pathsToDeleteByFileSystem : fsMap.asMap().entrySet()) {
      Path fsRoot = pathsToDeleteByFileSystem.getKey();
      deletionTasks.add(
          executorService()
              .submit(
                  () ->
                      deleteBatch(
                          Util.getFs(fsRoot, conf), fsRoot, pathsToDeleteByFileSystem.getValue())));
    }

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

    return totalFailedDeletions;
  }

  /**
   * Blocking batch delete.
   *
   * @param fs filesystem.
   * @param fsRoot root of the filesytem (all paths to delete must be under this).
   * @param paths paths to delete.
   * @return the list of paths which couldn't be deleted.
   * @throws UncheckedIOException if an IOE was raised in the invoked methods.
   */
  private List<Map.Entry<Path, String>> deleteBatch(
      FileSystem fs, final Path fsRoot, Collection<Path> paths) {

    LOG.debug("Deleting batch of {} files under {}", paths.size(), fsRoot);
    return wrappedIO.bulkDelete_delete(fs, fsRoot, paths);
  }

  private int deleteThreads() {
    int defaultValue = Runtime.getRuntime().availableProcessors() * DEFAULT_DELETE_CORE_MULTIPLE;
    return conf().getInt(DELETE_FILE_PARALLELISM, defaultValue);
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (HadoopFileIO.class) {
        if (executorService == null) {
          executorService =
              ThreadPools.newExitingWorkerPool(DELETE_FILE_POOL_NAME, deleteThreads());
        }
      }
    }

    return executorService;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  /**
   * This class is a simple adaptor to allow for using Hadoop's RemoteIterator as an Iterator. Also
   * forwards {@link #close()} to the delegate if it is a Closeable, and also {@link #toString()} as
   * some implementations report statistics there.
   *
   * @param <E> element type
   */
  private static class AdaptingIterator<E> implements Iterator<E>, RemoteIterator<E>, Closeable {
    private final RemoteIterator<E> delegate;

    AdaptingIterator(RemoteIterator<E> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      try {
        return delegate.hasNext();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public E next() {
      try {
        return delegate.next();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * If the delegate is a Closeable, this method will close it. Cloud stores with async fetching
     * may implement the operation As an example: S3A list calls will update the thread-level {@code
     * IOStatisticsContext} with the number and performance of list operations.
     *
     * <p>No-op if the iterator doesn't implement the API.
     *
     * @throws IOException exception if closed.
     */
    @Override
    public void close() throws IOException {
      if (delegate instanceof Closeable) {
        ((Closeable) delegate).close();
      }
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
