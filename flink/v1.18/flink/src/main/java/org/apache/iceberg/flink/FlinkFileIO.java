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
package org.apache.iceberg.flink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkFileIO implements FileIO, SupportsPrefixOperations, SupportsBulkOperations {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkFileIO.class);
  private static final String DELETE_FILE_PARALLELISM = "iceberg.hadoop.delete-file-parallelism";
  private static final String DELETE_FILE_POOL_NAME = "iceberg-hadoopfileio-delete";
  private static final int DELETE_RETRY_ATTEMPTS = 3;
  private static final int DEFAULT_DELETE_CORE_MULTIPLE = 4;
  private static volatile ExecutorService executorService;
  private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

  @Override
  public InputFile newInputFile(String path) {
    return new FlinkInputFile(new Path(path));
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new FlinkInputFile(new Path(path), length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new FlinkOutputFile(new Path(path));
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    try {
      toDelete.getFileSystem().delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to delete file: %s", path), e);
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    LOG.debug("Listing {}", prefix);
    Path prefixToList = new Path(prefix);
    try {
      return listPrefix(prefixToList.getFileSystem(), prefixToList);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void deletePrefix(String prefix) {
    Path prefixToDelete = new Path(prefix);

    try {
      prefixToDelete.getFileSystem().delete(prefixToDelete, true /* recursive */);
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

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  private Iterable<FileInfo> listPrefix(FileSystem fs, Path fileName) {
    try {
      FileStatus[] statuses = fs.listStatus(fileName);
      LOG.debug("Listing path {} {}", fileName, fs.listStatus(fileName));
      if (statuses == null) {
        // Check the file is there and ready. If so, then we can assume this is an empty dir.
        fs.getFileStatus(fileName);
        statuses = new FileStatus[0];
      }

      return Iterables.concat(
          Arrays.stream(statuses)
              .map(
                  fileStatus -> {
                    if (fileStatus.isDir()) {
                      return listPrefix(fs, fileStatus.getPath());
                    } else {
                      return Collections.singleton(
                          new FileInfo(
                              fileStatus.getPath().toString(),
                              fileStatus.getLen(),
                              fileStatus.getModificationTime()));
                    }
                  })
              .collect(Collectors.toSet()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (FlinkFileIO.class) {
        if (executorService == null) {
          executorService = ThreadPools.newWorkerPool(DELETE_FILE_POOL_NAME, deleteThreads());
        }
      }
    }

    return executorService;
  }

  private int deleteThreads() {
    int defaultValue = Runtime.getRuntime().availableProcessors() * DEFAULT_DELETE_CORE_MULTIPLE;
    return properties.containsKey(DELETE_FILE_PARALLELISM)
        ? Integer.parseInt(properties.get(DELETE_FILE_PARALLELISM))
        : defaultValue;
  }
}
