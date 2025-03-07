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
package org.apache.iceberg.vortex;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Properties for Vortex reads against Azure Data Lake Storage and Azure Blob Store. */
public class VortexMicrosoftProperties implements ObjectStoreProperties {
  private static final String ACCOUNT_NAME = "azure_storage_account_name";
  private static final String ACCOUNT_KEY = "azure_storage_account_key";
  private static final String CLIENT_ID = "azure_storage_client_id";
  private static final String CLIENT_SECRET = "azure_storage_client_secret";
  private static final String SAS_KEY = "azure_storage_sas_key";
  private static final String TOKEN = "azure_storage_token";
  private static final String ENDPOINT = "azure_storage_endpoint";
  private static final String SKIP_SIGNATURE = "azure_skip_signature";
  private static final String CONTAINER = "azure_container_name";

  private final Map<String, String> properties = Maps.newHashMap();

  @Override
  public Map<String, String> asProperties() {
    return Map.copyOf(properties);
  }

  public Optional<String> accountName() {
    return Optional.ofNullable(properties.get(ACCOUNT_NAME));
  }

  public Optional<String> accountKey() {
    return Optional.ofNullable(properties.get(ACCOUNT_KEY));
  }

  public Optional<String> clientId() {
    return Optional.ofNullable(properties.get(CLIENT_ID));
  }

  public Optional<String> clientSecret() {
    return Optional.ofNullable(properties.get(CLIENT_SECRET));
  }

  public Optional<String> sasKey() {
    return Optional.ofNullable(properties.get(SAS_KEY));
  }

  public Optional<String> token() {
    return Optional.ofNullable(properties.get(TOKEN));
  }

  public Optional<String> endpoint() {
    return Optional.ofNullable(properties.get(ENDPOINT));
  }

  public boolean skipSignature() {
    return Boolean.parseBoolean(properties.getOrDefault(SKIP_SIGNATURE, "false"));
  }

  public Optional<String> container() {
    return Optional.ofNullable(properties.get(CONTAINER));
  }

  public void setAccountName(String accountName) {
    properties.put(ACCOUNT_NAME, accountName);
  }

  public void setAccountKey(String accountKey) {
    properties.put(ACCOUNT_KEY, accountKey);
  }

  public void setClientId(String clientId) {
    properties.put(CLIENT_ID, clientId);
  }

  public void setClientSecret(String clientSecret) {
    properties.put(CLIENT_SECRET, clientSecret);
  }

  public void setSasKey(String sasKey) {
    properties.put(SAS_KEY, sasKey);
  }

  public void setToken(String token) {
    properties.put(TOKEN, token);
  }

  public void setEndpoint(String endpoint) {
    properties.put(ENDPOINT, endpoint);
  }

  public void setSkipSignature(boolean skipSignature) {
    properties.put(SKIP_SIGNATURE, Boolean.toString(skipSignature));
  }

  public void setContainer(String container) {
    properties.put(CONTAINER, container);
  }
}
