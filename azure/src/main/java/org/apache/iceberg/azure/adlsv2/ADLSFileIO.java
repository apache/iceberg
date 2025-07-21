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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FileIO implementation backed by Azure Data Lake Storage Gen2. */
public class ADLSFileIO implements DelegateFileIO, SupportsStorageCredentials {

  private static final Logger LOG = LoggerFactory.getLogger(ADLSFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static final String ABFS_PREFIX = "abfs";
  private static final String WASB_PREFIX = "wasb";

  private MetricsContext metrics = MetricsContext.nullMetrics();
  private SerializableMap<String, String> properties;
  // use modifiable collection for Kryo serde
  private List<StorageCredential> storageCredentials = Lists.newArrayList();
  private transient volatile Map<String, PrefixedADLSClient> clientByPrefix;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link ADLSFileIO#initialize(Map)} later.
   */
  public ADLSFileIO() {}

  @Override
  public InputFile newInputFile(String path) {
    return new ADLSInputFile(path, clientForStoragePath(path), metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new ADLSInputFile(path, length, clientForStoragePath(path), metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new ADLSOutputFile(path, clientForStoragePath(path), metrics);
  }

  @Override
  public void deleteFile(String path) {
    // There is no specific contract about whether delete should fail
    // and other FileIO providers ignore failure.  Log the failure for
    // now as it is not a required operation for Iceberg.
    try {
      clientForStoragePath(path).fileClient(path).delete();
    } catch (DataLakeStorageException e) {
      LOG.warn("Failed to delete path: {}", path, e);
    }
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  public DataLakeFileSystemClient client(String path) {
    return clientForStoragePath(path).client(path);
  }

  @VisibleForTesting
  PrefixedADLSClient clientForStoragePath(String path) {
    PrefixedADLSClient prefixedADLSClient;
    String matchingPrefix = ABFS_PREFIX;

    for (String storagePrefix : clientByPrefix().keySet()) {
      if (path.startsWith(storagePrefix) && storagePrefix.length() > matchingPrefix.length()) {
        matchingPrefix = storagePrefix;
      }
    }

    prefixedADLSClient = clientByPrefix().getOrDefault(matchingPrefix, null);

    Preconditions.checkState(
        null != prefixedADLSClient,
        "[BUG] ADLS client-builder for storage path not available: %s",
        path);
    return prefixedADLSClient;
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    initMetrics(properties);
  }

  @SuppressWarnings("CatchBlockLogException")
  private void initMetrics(Map<String, String> props) {
    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("adls");
      context.initialize(props);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics", DEFAULT_METRICS_IMPL);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    // Azure batch operations are not supported in all cases, e.g. with a user
    // delegation SAS token, so avoid using it for now

    AtomicInteger failureCount = new AtomicInteger();
    Tasks.foreach(pathsToDelete)
        .executeWith(ThreadPools.getWorkerPool())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure(
            (file, exc) -> {
              failureCount.incrementAndGet();
              LOG.warn("Failed to delete file {}", file, exc);
            })
        .run(this::deleteFile);

    if (failureCount.get() > 0) {
      throw new BulkDeletionFailureException(failureCount.get());
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    ADLSLocation location = new ADLSLocation(prefix);

    ListPathsOptions options = new ListPathsOptions();
    options.setPath(location.path());
    options.setRecursive(true);

    return () -> {
      try {
        return client(prefix).listPaths(options, null).stream()
            .filter(pathItem -> !pathItem.isDirectory())
            .map(
                pathItem ->
                    new FileInfo(
                        pathItem.getName(),
                        pathItem.getContentLength(),
                        pathItem.getCreationTime().toInstant().toEpochMilli()))
            .iterator();
      } catch (DataLakeStorageException e) {
        // other FileIO implementations return an empty iterator if nothing
        // is found, so mimic that behavior here
        if (e.getStatusCode() != 404) {
          throw e;
        }
        return Collections.emptyIterator();
      }
    };
  }

  @Override
  public void deletePrefix(String prefix) {
    ADLSLocation location = new ADLSLocation(prefix);
    try {
      client(prefix)
          .deleteDirectoryWithResponse(location.path(), true, null, null, Context.NONE)
          .getValue();
    } catch (DataLakeStorageException e) {
      // other FileIO implementations skip the delete if nothing is found,
      // so mimic that behavior here
      if (e.getStatusCode() != 404) {
        throw e;
      }
    }
  }

  @Override
  public void close() {
    if (clientByPrefix != null) {
      clientByPrefix.values().forEach(PrefixedADLSClient::close);
      this.clientByPrefix = null;
    }

    DelegateFileIO.super.close();
  }

  @Override
  public void setCredentials(List<StorageCredential> credentials) {
    Preconditions.checkArgument(credentials != null, "Invalid storage credentials: null");
    // copy credentials into a modifiable collection for Kryo serde
    this.storageCredentials = Lists.newArrayList(credentials);
  }

  @Override
  public List<StorageCredential> credentials() {
    return ImmutableList.copyOf(storageCredentials);
  }

  private Map<String, PrefixedADLSClient> clientByPrefix() {
    if (null == clientByPrefix) {
      synchronized (this) {
        if (null == clientByPrefix) {
          Map<String, PrefixedADLSClient> localClientByPrefix = Maps.newHashMap();

          PrefixedADLSClient rootBuilder = new PrefixedADLSClient(ABFS_PREFIX, properties);
          localClientByPrefix.put(ABFS_PREFIX, rootBuilder);
          localClientByPrefix.put(WASB_PREFIX, rootBuilder);

          this.storageCredentials.stream()
              .filter(c -> c.prefix().startsWith(ABFS_PREFIX) || c.prefix().startsWith(WASB_PREFIX))
              .collect(Collectors.toList())
              .forEach(
                  storageCredential -> {
                    Map<String, String> propertiesWithCredentials =
                        ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .putAll(storageCredential.config())
                            .buildKeepingLast();
                    localClientByPrefix.put(
                        storageCredential.prefix(),
                        new PrefixedADLSClient(
                            storageCredential.prefix(), propertiesWithCredentials));
                  });
          this.clientByPrefix = localClientByPrefix;
        }
      }
    }

    return clientByPrefix;
  }
}
