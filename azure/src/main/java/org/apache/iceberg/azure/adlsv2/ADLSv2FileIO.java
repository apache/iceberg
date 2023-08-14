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
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import java.util.Map;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.SerializableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FileIO implementation backed by Azure Data Lake Storage v2 (ADLSv2). */
public class ADLSv2FileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(ADLSv2FileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private static final HttpClient HTTP = HttpClient.createDefault();

  private AzureProperties azureProperties;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private SerializableMap<String, String> properties = null;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link ADLSv2FileIO#initialize(Map)} later.
   */
  public ADLSv2FileIO() {}

  @Override
  public InputFile newInputFile(String path) {
    return ADLSv2InputFile.of(path, client(path), azureProperties, metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return ADLSv2InputFile.of(path, length, client(path), azureProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return ADLSv2OutputFile.of(path, client(path), azureProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    // There is no specific contract about whether delete should fail
    // and other FileIO providers ignore failure.  Log the failure for
    // now as it is not a required operation for Iceberg.
    try {
      client(path).delete();
    } catch (Exception e) {
      LOG.warn("Failed to delete path: {}", path, e);
    }
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @VisibleForTesting
  DataLakeFileClient client(String path) {
    ADLSv2Location location = new ADLSv2Location(path);
    DataLakePathClientBuilder clientBuilder =
        new DataLakePathClientBuilder()
            .httpClient(HTTP)
            .endpoint(location.storageAccountUrl())
            .pathName(location.path());

    location.container().ifPresent(clientBuilder::fileSystemName);
    azureProperties.applyCredentialConfiguration(clientBuilder);

    return clientBuilder.buildFileClient();
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    this.azureProperties = new AzureProperties(properties);

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("abfs");
      context.initialize(properties);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics",
          DEFAULT_METRICS_IMPL,
          e);
    }
  }
}
