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
import java.util.AbstractMap;
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
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.wrappedio.WrappedIO;
import org.apache.iceberg.exceptions.RuntimeIOException;
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
  private static final String DELETE_FILE_POOL_NAME = "iceberg-hadoopfileio-delete";
  private static final int DELETE_RETRY_ATTEMPTS = 3;
  private static final int DEFAULT_DELETE_CORE_MULTIPLE = 4;
  private static volatile ExecutorService executorService;

  private volatile SerializableSupplier<Configuration> hadoopConf;
  private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

  /** Flag to indicate that Hadoop Bulk Delete API should be used. */
  @VisibleForTesting static final AtomicBoolean HADOOP_BULK_DELETE = new AtomicBoolean(true);

  // probe for WrappedIO class existing; if not found: disable bulk deletion.
  // because this version of hadoop is older than Hadoop 3.4.1
  static {
    try {
      HadoopFileIO.class.getClassLoader().loadClass("org.apache.hadoop.io.wrappedio.WrappedIO");
    } catch (ClassNotFoundException e) {
      LOG.debug("Failed to load WrappedIO class; likely older hadoop runtime", e);
      HADOOP_BULK_DELETE.set(false);
    }
  }

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
   * Delete files.
   *
   * @param pathsToDelete The paths to delete
   * @throws BulkDeletionFailureException failure to delete files.
   */
  @Override
  public void deleteFiles(final Iterable<String> pathsToDelete)
      throws BulkDeletionFailureException {
    Iterable<String> targetPaths = pathsToDelete;
    try {
      final List<Map.Entry<Path, String>> pathsNotDeleted = hadoopBulkDelete(targetPaths);
      if (pathsNotDeleted.isEmpty()) {
        return;
      }
      // one or more files were not deleted.
      targetPaths =
          pathsNotDeleted.stream()
              .map(
                  entry -> {
                    LOG.info("Failed to delete {} cause: {}", entry.getKey(), entry.getValue());
                    return entry.getKey().toString();
                  })
              .collect(Collectors.toList());
    } catch (RuntimeException e) {
      LOG.warn("Failed to use bulk delete -falling back to single delete calls", e);
    }

    AtomicInteger failureCount = new AtomicInteger(0);
    Tasks.foreach(targetPaths)
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
    if (failureCount.get() != 0) {
      throw new BulkDeletionFailureException(failureCount.get());
    }
  }

  /**
   * Delete files through the Hadoop Bulk Delete API.
   *
   * @param pathnames paths to delete.
   * @return All paths which could not be deleted, and the reason
   * @throws UncheckedIOException if an IOE was raised in the invoked methods.
   * @throws RuntimeException if interrupted while waiting for deletions to complete.
   */
  private List<Map.Entry<Path, String>> hadoopBulkDelete(Iterable<String> pathnames) {

    LOG.debug("Using bulk delete operation to delete files");

    SetMultimap<Path, Path> fsMap = Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
    Map<Path, Integer> fsPageSizeMap = Maps.newHashMap();
    List<Map.Entry<Path, String>> filesNotDeleted = Lists.newArrayList();
    List<Future<List<Map.Entry<Path, String>>>> deletionTasks = Lists.newArrayList();
    final Path rootPath = new Path("/");
    final Configuration conf = hadoopConf.get();
    for (String name : pathnames) {
      Path target = new Path(name);
      LOG.debug("Deleting '{}' mapped to path '{}'", name, target);
      final FileSystem fs;
      try {
        fs = Util.getFs(target, conf);
      } catch (Exception e) {
        LOG.warn("Failed to load filesystem for path: {}", target, e);
        filesNotDeleted.add(new AbstractMap.SimpleImmutableEntry<>(target, e.toString()));
        continue;
      }
      Path fsRoot = fs.makeQualified(rootPath);
      int pageSize;
      if (!fsPageSizeMap.containsKey(fsRoot)) {
        pageSize = WrappedIO.bulkDelete_pageSize(fs, rootPath);
        fsPageSizeMap.put(fsRoot, pageSize);
      } else {
        pageSize = fsPageSizeMap.get(fsRoot);
      }

      Set<Path> pathsForFilesystem = fsMap.get(fsRoot);
      Path targetPath = fs.makeQualified(target);
      pathsForFilesystem.add(targetPath);

      if (pathsForFilesystem.size() == pageSize) {
        Collection<Path> paths = Sets.newHashSet(pathsForFilesystem);
        deletionTasks.add(executorService().submit(() -> deleteBatch(fs, fsRoot, paths)));
        fsMap.removeAll(fsRoot);
      }
    }

    for (Map.Entry<Path, Collection<Path>> pathsToDeleteByFileSystem : fsMap.asMap().entrySet()) {
      Path fsRoot = pathsToDeleteByFileSystem.getKey();
      deletionTasks.add(
          executorService()
              .submit(
                  () ->
                      deleteBatch(
                          Util.getFs(fsRoot, conf), fsRoot, pathsToDeleteByFileSystem.getValue())));
    }

    LOG.debug("Waiting for {} deletion tasks to complete", deletionTasks.size());

    for (Future<List<Map.Entry<Path, String>>> deletionTask : deletionTasks) {
      try {
        List<Map.Entry<Path, String>> failedDeletions = deletionTask.get();
        failedDeletions.forEach(
            entry -> {
              LOG.debug("Failed to delete object at path {}: {}", entry.getKey(), entry.getValue());
              filesNotDeleted.add(entry);
            });
      } catch (ExecutionException e) {
        LOG.warn("Exception during batch deletion", e.getCause());
        // this failure
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        deletionTasks.stream().filter(task -> !task.isDone()).forEach(task -> task.cancel(true));
        throw new RuntimeException("Interrupted when waiting for deletions to complete", e);
      }
    }
    return filesNotDeleted;
  }

  /**
   * Blocking batch delete.
   *
   * @param fs filesystem.
   * @param fsRoot root of the filesytem
   * @param paths paths to delete.
   * @return Paths which couldn't be deleted and the error messages
   * @throws UncheckedIOException IO problem
   */
  private List<Map.Entry<Path, String>> deleteBatch(
      FileSystem fs, final Path fsRoot, Collection<Path> paths) {

    LOG.debug("Deleting batch of {} files under {}", paths.size(), fsRoot);
    return WrappedIO.bulkDelete_delete(fs, fsRoot, paths);
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

  /**
   * This class is a simple adaptor to allow for using Hadoop's RemoteIterator as an Iterator.
   * Forwards {@link #close()} to the delegate if it is Closeable.
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
