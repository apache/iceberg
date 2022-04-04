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

package org.apache.iceberg.azure;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class AzureProperties implements Serializable {

  // Start of storage account configuration
  public static final String STORAGE_CONNECTION_STRING = "abfs.%s.connection-string";

  public static final String STORAGE_ENDPOINT = "abfs.%s.endpoint";

  public static final String STORAGE_AUTH_TYPE = "abfs.%s.auth-type";

  public static final String STORAGE_ACCOUNT_KEY = "abfs.%s.account-key";

  public static final String STORAGE_SHARED_ACCESS_SIGNATURE = "abfs.%s.shared-access-signature";

  public static final String STORAGE_READ_BLOCK_SIZE = "abfs.%s.read.block-size";
  public static final Integer STORAGE_READ_BLOCK_SIZE_DEFAULT = 8 * 1024 * 1024;

  public static final String STORAGE_WRITE_BLOCK_SIZE = "abfs.%s.write.block-size";
  public static final Long STORAGE_WRITE_BLOCK_SIZE_DEFAULT = 4L * 1024 * 1024;

  public static final String STORAGE_WRITE_MAX_CONCURRENCY = "abfs.%s.write.max-concurrency";
  public static final Integer STORAGE_WRITE_MAX_CONCURRENCY_DEFAULT = 8;

  public static final String STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE = "abfs.%s.write.max-single-upload-size";
  public static final Long STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE_DEFAULT = 4L * 1024 * 1024;
  // End of storage account configurations

  private final Map<String, String> properties;

  public AzureProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties map cannot be null");
    // Immutable Map raises exceptions during kryo serialization in Spark, thus using unmodifiable map.
    this.properties = Collections.unmodifiableMap(Maps.newHashMap(properties));
  }

  public AuthType authType(String storageAccount) {
    final Optional<String> authType = getProperty(storageAccount, STORAGE_AUTH_TYPE);
    return authType.map(AuthType::valueOf).orElse(AuthType.None);
  }

  public Optional<String> connectionString(String storageAccount) {
    return getProperty(storageAccount, STORAGE_CONNECTION_STRING);
  }

  public Optional<String> endpoint(String storageAccount) {
    return getProperty(storageAccount, STORAGE_ENDPOINT);
  }

  public Optional<String> accountKey(String storageAccount) {
    return getProperty(storageAccount, STORAGE_ACCOUNT_KEY);
  }

  public Optional<String> sharedAccessSignature(String storageAccount) {
    return getProperty(storageAccount, STORAGE_SHARED_ACCESS_SIGNATURE);
  }

  public Integer readBlockSize(String storageAccount) {
    return getProperty(storageAccount, STORAGE_READ_BLOCK_SIZE).map(Integer::parseInt)
        .orElse(STORAGE_READ_BLOCK_SIZE_DEFAULT);
  }

  public Long writeBlockSize(String storageAccount) {
    return getProperty(storageAccount, STORAGE_WRITE_BLOCK_SIZE).map(Long::parseLong)
        .orElse(STORAGE_WRITE_BLOCK_SIZE_DEFAULT);
  }

  public Integer maxWriteConcurrency(String storageAccount) {
    return getProperty(storageAccount, STORAGE_WRITE_MAX_CONCURRENCY).map(Integer::parseInt)
        .orElse(STORAGE_WRITE_MAX_CONCURRENCY_DEFAULT);
  }

  public Long maxSingleUploadSize(String storageAccount) {
    return getProperty(storageAccount, STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE).map(Long::parseLong)
        .orElse(STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE_DEFAULT);
  }

  private Optional<String> getProperty(String storageAccount, String propertyTemplate) {
    Preconditions.checkArgument(
        storageAccount != null && !storageAccount.isEmpty(),
        "Storage Account cannot be null or empty");
    return Optional.ofNullable(properties.get(String.format(propertyTemplate, storageAccount)));
  }
}
