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
package org.apache.iceberg.gcp.gcs;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.gcp.GCPAuthUtils;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;

class PrefixedStorage implements AutoCloseable {
  private static final String GCS_FILE_IO_USER_AGENT = "gcsfileio/" + EnvironmentContext.get();
  private final String storagePrefix;
  private final GCPProperties gcpProperties;
  private SerializableSupplier<Storage> storage;
  private CloseableGroup closeableGroup;
  private transient volatile Storage storageClient;
  private final SerializableSupplier<GcsFileSystem> gcsFileSystemSupplier;
  private transient volatile GcsFileSystem gcsFileSystem;

  PrefixedStorage(
      String storagePrefix, Map<String, String> properties, SerializableSupplier<Storage> storage) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(storagePrefix), "Invalid storage prefix: null or empty");
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    this.storagePrefix = storagePrefix;
    this.storage = storage;
    this.gcpProperties = new GCPProperties(properties);
    this.closeableGroup = new CloseableGroup();
    if (null == storage) {
      this.storage =
          () -> {
            StorageOptions.Builder builder =
                StorageOptions.newBuilder()
                    .setHeaderProvider(
                        FixedHeaderProvider.create(
                            ImmutableMap.of("User-agent", GCS_FILE_IO_USER_AGENT)));

            gcpProperties.projectId().ifPresent(builder::setProjectId);
            gcpProperties.clientLibToken().ifPresent(builder::setClientLibToken);
            gcpProperties.serviceHost().ifPresent(builder::setHost);

            Credentials credentials = credentials(gcpProperties);
            if (credentials != null) {
              builder.setCredentials(credentials);
            }

            return builder.build().getService();
          };
    }

    this.gcsFileSystemSupplier = gcsFileSystemSupplier(properties);
  }

  public String storagePrefix() {
    return storagePrefix;
  }

  public Storage storage() {
    if (null == storageClient) {
      synchronized (this) {
        if (null == storageClient) {
          this.storageClient = storage.get();
        }
      }
    }

    return storageClient;
  }

  public GCPProperties gcpProperties() {
    return gcpProperties;
  }

  @Override
  public void close() {
    if (null != closeableGroup) {
      try {
        closeableGroup.close();
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }

    if (null != storage) {
      // GCS Storage does not appear to be closable, so release the reference
      storage = null;
    }
  }

  GcsFileSystem gcsFileSystem() {
    if (gcsFileSystem == null) {
      synchronized (this) {
        if (gcsFileSystem == null) {
          this.gcsFileSystem = gcsFileSystemSupplier.get();
          this.closeableGroup.addCloseable(gcsFileSystem);
        }
      }
    }
    return this.gcsFileSystem;
  }

  private Credentials credentials(GCPProperties properties) {
    // Google Cloud APIs default to automatically detect the credentials to use, which is
    // in most cases the convenient way, especially in GCP.
    // See javadoc of com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
    if (properties.oauth2Token().isPresent()) {
      return GCPAuthUtils.oauth2CredentialsFromGcpProperties(properties, closeableGroup);
    } else if (properties.noAuth()) {
      // Explicitly allow "no credentials" for testing purposes
      return NoCredentials.getInstance();
    } else {
      return null;
    }
  }

  private SerializableSupplier<GcsFileSystem> gcsFileSystemSupplier(
      Map<String, String> properties) {
    ImmutableMap.Builder<String, String> propertiesWithUserAgent =
        new ImmutableMap.Builder<String, String>()
            .putAll(properties)
            .put("gcs.user-agent", GCS_FILE_IO_USER_AGENT);
    GcsAnalyticsCoreOptions gcsAnalyticsCoreOptions =
        new GcsAnalyticsCoreOptions("gcs.", propertiesWithUserAgent.build());
    GcsFileSystemOptions fileSystemOptions = gcsAnalyticsCoreOptions.getGcsFileSystemOptions();
    Credentials credentials = credentials(new GCPProperties(properties));
    return () ->
        credentials == null
            ? new GcsFileSystemImpl(fileSystemOptions)
            : new GcsFileSystemImpl(credentials, fileSystemOptions);
  }
}
