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

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class AzureProperties implements Serializable {
  public static final String ADLSV2_READ_BLOCK_SIZE = "adlsv2.read.block-size-bytes";
  public static final String ADLSV2_WRITE_BLOCK_SIZE = "adlsv2.write.block-size-bytes";
  private static final DefaultAzureCredential DEFAULT_CREDENTIAL =
      new DefaultAzureCredentialBuilder().build();

  private Integer adlsv2ReadBlockSize;
  private Long adlsv2WriteBlockSize;

  public AzureProperties() {}

  public AzureProperties(Map<String, String> properties) {
    if (properties.containsKey(ADLSV2_READ_BLOCK_SIZE)) {
      adlsv2ReadBlockSize = Integer.parseInt(properties.get(ADLSV2_READ_BLOCK_SIZE));
    }
    if (properties.containsKey(ADLSV2_WRITE_BLOCK_SIZE)) {
      adlsv2WriteBlockSize = Long.parseLong(properties.get(ADLSV2_WRITE_BLOCK_SIZE));
    }
  }

  public Optional<Integer> adlsv2ReadBlockSize() {
    return Optional.ofNullable(adlsv2ReadBlockSize);
  }

  public Optional<Long> adlsv2WriteBlockSize() {
    return Optional.ofNullable(adlsv2WriteBlockSize);
  }

  public <T extends DataLakePathClientBuilder> void applyCredentialConfiguration(T builder) {
    builder.credential(DEFAULT_CREDENTIAL);
  }
}
