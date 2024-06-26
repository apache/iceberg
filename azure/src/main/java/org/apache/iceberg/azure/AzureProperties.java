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

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

public class AzureProperties implements Serializable {
  public static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  public static final String ADLS_CONNECTION_STRING_PREFIX = "adls.connection-string.";
  public static final String ADLS_READ_BLOCK_SIZE = "adls.read.block-size-bytes";
  public static final String ADLS_WRITE_BLOCK_SIZE = "adls.write.block-size-bytes";
  public static final String ADLS_SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";
  public static final String ADLS_SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  private Map<String, String> adlsSasTokens = Collections.emptyMap();
  private Map<String, String> adlsConnectionStrings = Collections.emptyMap();
  private Map.Entry<String, String> namedKeyCreds;
  private Integer adlsReadBlockSize;
  private Long adlsWriteBlockSize;

  public AzureProperties() {}

  public AzureProperties(Map<String, String> properties) {
    this.adlsSasTokens = PropertyUtil.propertiesWithPrefix(properties, ADLS_SAS_TOKEN_PREFIX);
    this.adlsConnectionStrings =
        PropertyUtil.propertiesWithPrefix(properties, ADLS_CONNECTION_STRING_PREFIX);

    String sharedKeyAccountName = properties.get(ADLS_SHARED_KEY_ACCOUNT_NAME);
    String sharedKeyAccountKey = properties.get(ADLS_SHARED_KEY_ACCOUNT_KEY);
    if (sharedKeyAccountName != null || sharedKeyAccountKey != null) {
      Preconditions.checkArgument(
          sharedKeyAccountName != null && sharedKeyAccountKey != null,
          "Azure authentication: shared-key requires both %s and %s",
          ADLS_SHARED_KEY_ACCOUNT_NAME,
          ADLS_SHARED_KEY_ACCOUNT_KEY);
      this.namedKeyCreds = Maps.immutableEntry(sharedKeyAccountName, sharedKeyAccountKey);
    }

    if (properties.containsKey(ADLS_READ_BLOCK_SIZE)) {
      this.adlsReadBlockSize = Integer.parseInt(properties.get(ADLS_READ_BLOCK_SIZE));
    }
    if (properties.containsKey(ADLS_WRITE_BLOCK_SIZE)) {
      this.adlsWriteBlockSize = Long.parseLong(properties.get(ADLS_WRITE_BLOCK_SIZE));
    }
  }

  public Optional<Integer> adlsReadBlockSize() {
    return Optional.ofNullable(adlsReadBlockSize);
  }

  public Optional<Long> adlsWriteBlockSize() {
    return Optional.ofNullable(adlsWriteBlockSize);
  }

  public void applyClientConfiguration(String account, DataLakeFileSystemClientBuilder builder) {
    String sasToken = adlsSasTokens.get(account);
    if (sasToken != null && !sasToken.isEmpty()) {
      builder.sasToken(sasToken);
    } else if (namedKeyCreds != null) {
      builder.credential(
          new StorageSharedKeyCredential(namedKeyCreds.getKey(), namedKeyCreds.getValue()));
    } else {
      builder.credential(new DefaultAzureCredentialBuilder().build());
    }

    // apply connection string last so its parameters take precedence, e.g. SAS token
    String connectionString = adlsConnectionStrings.get(account);
    if (connectionString != null && !connectionString.isEmpty()) {
      builder.endpoint(connectionString);
    } else {
      builder.endpoint("https://" + account);
    }
  }
}
