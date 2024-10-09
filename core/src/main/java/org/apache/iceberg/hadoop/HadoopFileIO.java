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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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

  private SerializableSupplier<Configuration> hadoopConf;
  private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

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
    return HadoopInputFile.fromLocation(path, hadoopConf.get());
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return HadoopInputFile.fromLocation(path, length, hadoopConf.get());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return HadoopOutputFile.fromPath(new Path(path), hadoopConf.get());
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFs(toDelete, hadoopConf.get());
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
    // E.g. newInputFile would throw NPE with hadoopConf.get() otherwise.
    if (hadoopConf == null) {
      this.hadoopConf = new SerializableConfiguration(new Configuration())::get;
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
    FileSystem fs = Util.getFs(prefixToList, hadoopConf.get());

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
    FileSystem fs = Util.getFs(prefixToDelete, hadoopConf.get());

    try {
      fs.delete(prefixToDelete, true /* recursive */);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    AtomicInteger failureCount = new AtomicInteger(0);
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

    if (failureCount.get() != 0) {
      throw new BulkDeletionFailureException(failureCount.get());
    }
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
   *
   * @param <E> element type
   */
  private static class AdaptingIterator<E> implements Iterator<E>, RemoteIterator<E> {
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
  }
}
