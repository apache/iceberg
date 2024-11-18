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

import com.azure.core.credential.AzureSasCredential;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VendedAzureSasCredentialProvider implements Serializable, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(VendedAzureSasCredentialProvider.class);

  private final SerializableMap<String, String> properties;
  private transient volatile Map<String, AzureSasCredentialRefresher>
      azureSasCredentialRefresherMap;
  private transient volatile RESTClient client;
  private transient volatile ScheduledExecutorService refreshExecutor;

  public static final String URI = "credentials.uri";
  private static final String THREAD_PREFIX = "adls-fileio-credential-refresh";

  public VendedAzureSasCredentialProvider(Map<String, String> properties) {
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    Preconditions.checkArgument(null != properties.get(URI), "Invalid URI: null");
    this.properties = SerializableMap.copyOf(properties);
    azureSasCredentialRefresherMap = Maps.newHashMap();
  }

  public AzureSasCredential getCredential(String storageAccount) {
    Map<String, AzureSasCredentialRefresher> refresherMap = azureSasCredentialRefresherMap();
    if (refresherMap.containsKey(storageAccount)) {
      return refresherMap.get(storageAccount).get();
    } else {
      AzureSasCredentialRefresher azureSasCredentialRefresher =
          new AzureSasCredentialRefresher(
              () -> this.getSasTokenWithExpiration(storageAccount), credentialRefreshExecutor());
      refresherMap.put(storageAccount, azureSasCredentialRefresher);
      return azureSasCredentialRefresher.get();
    }
  }

  private Pair<String, Long> getSasTokenWithExpiration(String storageAccount) {
    LoadCredentialsResponse response = fetchCredentials();
    List<Credential> adlsCredentials =
        response.credentials().stream()
            .filter(c -> c.prefix().contains(storageAccount))
            .collect(Collectors.toList());
    Preconditions.checkState(
        !adlsCredentials.isEmpty(),
        String.format("Invalid ADLS Credentials for storage-account %s: empty", storageAccount));
    Preconditions.checkState(
        adlsCredentials.size() == 1,
        "Invalid ADLS Credentials: only one ADLS credential should exist per storage-account");

    Credential adlsCredential = adlsCredentials.get(0);
    checkCredential(adlsCredential, AzureProperties.ADLS_SAS_TOKEN_PREFIX + storageAccount);
    checkCredential(
        adlsCredential, AzureProperties.ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + storageAccount);

    String updatedSasToken =
        adlsCredential.config().get(AzureProperties.ADLS_SAS_TOKEN_PREFIX + storageAccount);
    String tokenExpiresAtMillis =
        adlsCredential
            .config()
            .get(AzureProperties.ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + storageAccount);

    Long expiresAtMs = Long.parseLong(tokenExpiresAtMillis);

    return Pair.of(updatedSasToken, expiresAtMs);
  }

  private Map<String, AzureSasCredentialRefresher> azureSasCredentialRefresherMap() {
    if (this.azureSasCredentialRefresherMap == null) {
      synchronized (this) {
        if (this.azureSasCredentialRefresherMap == null) {
          this.azureSasCredentialRefresherMap = Maps.newHashMap();
        }
      }
    }
    return this.azureSasCredentialRefresherMap;
  }

  private ScheduledExecutorService credentialRefreshExecutor() {
    if (this.refreshExecutor == null) {
      synchronized (this) {
        if (this.refreshExecutor == null) {
          this.refreshExecutor = ThreadPools.newScheduledPool(THREAD_PREFIX, 1);
        }
      }
    }
    return this.refreshExecutor;
  }

  private RESTClient httpClient() {
    if (null == client) {
      synchronized (this) {
        if (null == client) {
          client = HTTPClient.builder(properties).uri(properties.get(URI)).build();
        }
      }
    }

    return client;
  }

  private LoadCredentialsResponse fetchCredentials() {
    return httpClient()
        .get(
            properties.get(URI),
            null,
            LoadCredentialsResponse.class,
            OAuth2Util.authHeaders(properties.get(OAuth2Properties.TOKEN)),
            ErrorHandlers.defaultErrorHandler());
  }

  private void checkCredential(Credential credential, String property) {
    Preconditions.checkState(
        credential.config().containsKey(property),
        "Invalid ADLS Credentials: %s not set",
        property);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(client);
    shutdownRefreshExecutor();
  }

  private void shutdownRefreshExecutor() {
    if (refreshExecutor != null) {
      ScheduledExecutorService service = refreshExecutor;
      this.refreshExecutor = null;

      List<Runnable> tasks = service.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });

      try {
        if (!service.awaitTermination(1, TimeUnit.MINUTES)) {
          LOG.warn("Timed out waiting for refresh executor to terminate");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for refresh executor to terminate", e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
