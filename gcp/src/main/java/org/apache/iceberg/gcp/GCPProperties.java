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
package org.apache.iceberg.gcp;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class GCPProperties implements Serializable {
  // For Google Cloud Storage (GCS).
  // Service Options
  public static final String GCS_PROJECT_ID = "gcs.project-id";
  public static final String GCS_CLIENT_LIB_TOKEN = "gcs.client-lib-token";
  public static final String GCS_SERVICE_HOST = "gcs.service.host";

  // GCS Configuration Properties
  public static final String GCS_DECRYPTION_KEY = "gcs.decryption-key";
  public static final String GCS_ENCRYPTION_KEY = "gcs.encryption-key";
  public static final String GCS_USER_PROJECT = "gcs.user-project";

  public static final String GCS_CHANNEL_READ_CHUNK_SIZE = "gcs.channel.read.chunk-size-bytes";
  public static final String GCS_CHANNEL_WRITE_CHUNK_SIZE = "gcs.channel.write.chunk-size-bytes";

  // For BigQuery BigLake Metastore.
  // The endpoint of BigLake API.
  // Optional, default to BigLakeCatalog.DEFAULT_BIGLAKE_SERVICE_ENDPOINT.
  public static final String BIGLAKE_ENDPOINT = "biglake.endpoint";
  // The GCP project ID. Required.
  public static final String BIGLAKE_PROJECT_ID = "biglake.project-id";
  // The GCP region (https://cloud.google.com/bigquery/docs/locations). Required.
  public static final String BIGLAKE_GCP_REGION = "biglake.region";
  // The BigLake Metastore catalog ID. It is the container resource of databases and tables.
  // It links a BLMS catalog with this Iceberg catalog.
  // Optional, default to the Spark catalog plugin name.
  public static final String BIGLAKE_CATALOG_ID = "biglake.catalog-id";

  private String projectId;
  private String clientLibToken;
  private String serviceHost;

  private String gcsDecryptionKey;
  private String gcsEncryptionKey;
  private String gcsUserProject;

  private Integer gcsChannelReadChunkSize;
  private Integer gcsChannelWriteChunkSize;

  public GCPProperties() {}

  public GCPProperties(Map<String, String> properties) {
    projectId = properties.get(GCS_PROJECT_ID);
    clientLibToken = properties.get(GCS_CLIENT_LIB_TOKEN);
    serviceHost = properties.get(GCS_SERVICE_HOST);

    gcsDecryptionKey = properties.get(GCS_DECRYPTION_KEY);
    gcsEncryptionKey = properties.get(GCS_ENCRYPTION_KEY);
    gcsUserProject = properties.get(GCS_USER_PROJECT);

    if (properties.containsKey(GCS_CHANNEL_READ_CHUNK_SIZE)) {
      gcsChannelReadChunkSize = Integer.parseInt(properties.get(GCS_CHANNEL_READ_CHUNK_SIZE));
    }

    if (properties.containsKey(GCS_CHANNEL_WRITE_CHUNK_SIZE)) {
      gcsChannelWriteChunkSize = Integer.parseInt(properties.get(GCS_CHANNEL_WRITE_CHUNK_SIZE));
    }
  }

  public Optional<Integer> channelReadChunkSize() {
    return Optional.ofNullable(gcsChannelReadChunkSize);
  }

  public Optional<Integer> channelWriteChunkSize() {
    return Optional.ofNullable(gcsChannelWriteChunkSize);
  }

  public Optional<String> clientLibToken() {
    return Optional.ofNullable(clientLibToken);
  }

  public Optional<String> decryptionKey() {
    return Optional.ofNullable(gcsDecryptionKey);
  }

  public Optional<String> encryptionKey() {
    return Optional.ofNullable(gcsEncryptionKey);
  }

  public Optional<String> projectId() {
    return Optional.ofNullable(projectId);
  }

  public Optional<String> serviceHost() {
    return Optional.ofNullable(serviceHost);
  }

  public Optional<String> userProject() {
    return Optional.ofNullable(gcsUserProject);
  }
}
