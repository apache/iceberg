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
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.util.PropertyUtil;

public class GCPProperties implements Serializable {
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

  public static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
  public static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";
  // Boolean to explicitly configure "no authentication" for testing purposes using a GCS emulator
  public static final String GCS_NO_AUTH = "gcs.no-auth";
  public static final String GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT =
      "gcs.oauth2.refresh-credentials-endpoint";

  /** Controls whether vended credentials should be refreshed or not. Defaults to true. */
  public static final String GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED =
      "gcs.oauth2.refresh-credentials-enabled";

  /** Configure the batch size used when deleting multiple files from a given GCS bucket */
  public static final String GCS_DELETE_BATCH_SIZE = "gcs.delete.batch-size";

  /** Controls whether analytics core library is enabled or not. Defaults to false. */
  public static final String GCS_ANALYTICS_CORE_ENABLED = "gcs.analytics-core.enabled";

  /**
   * Max possible batch size for deletion. Currently, a max of 100 keys is advised, so we default to
   * a number below that. https://cloud.google.com/storage/docs/batch
   */
  public static final int GCS_DELETE_BATCH_SIZE_DEFAULT = 50;

  private final Map<String, String> allProperties;

  private String projectId;
  private String clientLibToken;
  private String serviceHost;

  private String gcsDecryptionKey;
  private String gcsEncryptionKey;
  private String gcsUserProject;

  private Integer gcsChannelReadChunkSize;
  private Integer gcsChannelWriteChunkSize;

  private boolean gcsNoAuth;
  private String gcsOAuth2Token;
  private Date gcsOAuth2TokenExpiresAt;
  private String gcsOauth2RefreshCredentialsEndpoint;
  private boolean gcsOauth2RefreshCredentialsEnabled;
  private boolean gcsAnalyticsCoreEnabled;

  private int gcsDeleteBatchSize = GCS_DELETE_BATCH_SIZE_DEFAULT;

  public GCPProperties() {
    this.allProperties = ImmutableMap.of();
  }

  @SuppressWarnings("JavaUtilDate") // GCP API uses java.util.Date
  public GCPProperties(Map<String, String> properties) {
    this.allProperties = ImmutableMap.copyOf(properties);
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

    gcsOAuth2Token = properties.get(GCS_OAUTH2_TOKEN);
    if (properties.containsKey(GCS_OAUTH2_TOKEN_EXPIRES_AT)) {
      gcsOAuth2TokenExpiresAt =
          new Date(Long.parseLong(properties.get(GCS_OAUTH2_TOKEN_EXPIRES_AT)));
    }

    gcsOauth2RefreshCredentialsEndpoint =
        RESTUtil.resolveEndpoint(
            properties.get(CatalogProperties.URI),
            properties.get(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
    gcsOauth2RefreshCredentialsEnabled =
        PropertyUtil.propertyAsBoolean(properties, GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, true);
    gcsNoAuth = Boolean.parseBoolean(properties.getOrDefault(GCS_NO_AUTH, "false"));
    Preconditions.checkState(
        !(gcsOAuth2Token != null && gcsNoAuth),
        "Invalid auth settings: must not configure %s and %s",
        GCS_NO_AUTH,
        GCS_OAUTH2_TOKEN);

    gcsDeleteBatchSize =
        PropertyUtil.propertyAsInt(
            properties, GCS_DELETE_BATCH_SIZE, GCS_DELETE_BATCH_SIZE_DEFAULT);
    gcsAnalyticsCoreEnabled =
        PropertyUtil.propertyAsBoolean(properties, GCS_ANALYTICS_CORE_ENABLED, false);
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

  public Optional<String> oauth2Token() {
    return Optional.ofNullable(gcsOAuth2Token);
  }

  public boolean noAuth() {
    return gcsNoAuth;
  }

  public Optional<Date> oauth2TokenExpiresAt() {
    return Optional.ofNullable(gcsOAuth2TokenExpiresAt);
  }

  public int deleteBatchSize() {
    return gcsDeleteBatchSize;
  }

  public Optional<String> oauth2RefreshCredentialsEndpoint() {
    return Optional.ofNullable(gcsOauth2RefreshCredentialsEndpoint);
  }

  public boolean oauth2RefreshCredentialsEnabled() {
    return gcsOauth2RefreshCredentialsEnabled;
  }

  public Map<String, String> properties() {
    return allProperties;
  }

  public boolean isGcsAnalyticsCoreEnabled() {
    return gcsAnalyticsCoreEnabled;
  }
}
