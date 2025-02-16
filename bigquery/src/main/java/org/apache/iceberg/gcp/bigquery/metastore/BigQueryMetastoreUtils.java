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
package org.apache.iceberg.gcp.bigquery.metastore;

import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.SerDeInfo;
import com.google.api.services.bigquery.model.StorageDescriptor;
import java.util.Map;

/** Shared utilities for BigQuery Metastore specific functions and constants. */
public final class BigQueryMetastoreUtils {

  private BigQueryMetastoreUtils() {}

  // TODO: Consider using "org.apache.iceberg.mr.hive.HiveIcebergSerDe" when
  // TableProperties.ENGINE_HIVE_ENABLED is set.
  public static final String SERIALIZATION_LIBRARY =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  public static final String FILE_INPUT_FORMAT = "org.apache.hadoop.mapred.FileInputFormat";
  public static final String FILE_OUTPUT_FORMAT = "org.apache.hadoop.mapred.FileOutputFormat";

  /**
   * Creates a new ExternalCatalogTableOptions object populated with the supported library constants
   * and parameters given.
   *
   * @param locationUri storage location uri
   * @param parameters table metadata parameters
   */
  public static ExternalCatalogTableOptions createExternalCatalogTableOptions(
      String locationUri, Map<String, String> parameters) {
    return new ExternalCatalogTableOptions()
        .setStorageDescriptor(createStorageDescriptor(locationUri))
        .setParameters(parameters);
  }

  /**
   * Creates a new ExternalCatalogDatasetOptions object populated with the supported library
   * constants and parameters given.
   *
   * @param defaultStorageLocationUri dataset's default location uri
   * @param metadataParameters metadata parameters for the dataset
   */
  public static ExternalCatalogDatasetOptions createExternalCatalogDatasetOptions(
      String defaultStorageLocationUri, Map<String, String> metadataParameters) {
    return new ExternalCatalogDatasetOptions()
        .setDefaultStorageLocationUri(defaultStorageLocationUri)
        .setParameters(metadataParameters);
  }

  private static StorageDescriptor createStorageDescriptor(String locationUri) {
    SerDeInfo serDeInfo = new SerDeInfo().setSerializationLibrary(SERIALIZATION_LIBRARY);

    return new StorageDescriptor()
        .setLocationUri(locationUri)
        .setInputFormat(FILE_INPUT_FORMAT)
        .setOutputFormat(FILE_OUTPUT_FORMAT)
        .setSerdeInfo(serDeInfo);
  }
}
