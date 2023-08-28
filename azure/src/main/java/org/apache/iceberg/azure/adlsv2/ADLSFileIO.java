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

import com.azure.core.http.HttpClient;
import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FileIO implementation backed by Azure Data Lake Storage Gen2. */
public class ADLSFileIO implements FileIO, SupportsBulkOperations, SupportsPrefixOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ADLSFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private static final HttpClient HTTP = HttpClient.createDefault();

  private AzureProperties azureProperties;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private SerializableMap<String, String> properties;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link ADLSFileIO#initialize(Map)} later.
   */
  public ADLSFileIO() {}

  @VisibleForTesting
  ADLSFileIO(AzureProperties azureProperties) {
    this.azureProperties = azureProperties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new ADLSInputFile(path, fileClient(path), azureProperties, metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new ADLSInputFile(path, length, fileClient(path), azureProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new ADLSOutputFile(path, fileClient(path), azureProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    // There is no specific contract about whether delete should fail
    // and other FileIO providers ignore failure.  Log the failure for
    // now as it is not a required operation for Iceberg.
    try {
      fileClient(path).delete();
    } catch (DataLakeStorageException e) {
      LOG.warn("Failed to delete path: {}", path, e);
    }
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  public DataLakeFileSystemClient client(String path) {
    ADLSLocation location = new ADLSLocation(path);
    return client(location);
  }

  @VisibleForTesting
  DataLakeFileSystemClient client(ADLSLocation location) {
    DataLakeFileSystemClientBuilder clientBuilder =
        new DataLakeFileSystemClientBuilder().httpClient(HTTP);

    location.container().ifPresent(clientBuilder::fileSystemName);
    azureProperties.applyClientConfiguration(location.storageAccount(), clientBuilder);

    return clientBuilder.buildClient();
  }

  private DataLakeFileClient fileClient(String path) {
    ADLSLocation location = new ADLSLocation(path);
    return client(location).getFileClient(location.path());
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    this.azureProperties = new AzureProperties(properties);
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
        return client(location).listPaths(options, null).stream()
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
      client(location)
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
}
