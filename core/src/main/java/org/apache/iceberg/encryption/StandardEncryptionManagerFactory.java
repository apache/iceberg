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
package org.apache.iceberg.encryption;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

public class StandardEncryptionManagerFactory implements EncryptionManagerFactory {
  private KeyManagementClient kmsClient;
  private Map<String, String> catalogPropertyMap;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.catalogPropertyMap = catalogProperties;
  }

  @Override
  public EncryptionManager create(TableMetadata tableMetadata) {
    if (tableMetadata == null) {
      return PlaintextEncryptionManager.instance();
    }

    Map<String, String> tableProperties = tableMetadata.properties();

    final Map<String, String> encryptionProperties = Maps.newHashMap();
    encryptionProperties.putAll(tableProperties);

    // Important: put catalog properties after table properties. Former overrides the latter.
    encryptionProperties.putAll(catalogPropertyMap);

    String tableKeyId = encryptionProperties.get(EncryptionProperties.ENCRYPTION_TABLE_KEY);

    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.instance();
    } else {
      String fileFormat =
          PropertyUtil.propertyAsString(
              tableProperties, DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);

      if (FileFormat.fromString(fileFormat) != FileFormat.PARQUET) {
        throw new UnsupportedOperationException(
            "Iceberg encryption currently supports only parquet format for data files");
      }

      int dataKeyLength =
          PropertyUtil.propertyAsInt(
              encryptionProperties,
              EncryptionProperties.ENCRYPTION_DEK_LENGTH,
              EncryptionProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);

      return new StandardEncryptionManager(
          tableKeyId, dataKeyLength, kmsClient(encryptionProperties), encryptionProperties);
    }
  }

  private synchronized KeyManagementClient kmsClient(Map<String, String> encryptionProperties) {
    if (kmsClient == null) {
      String kmsImpl = encryptionProperties.get(EncryptionProperties.ENCRYPTION_KMS_CLIENT_IMPL);

      Preconditions.checkArgument(
          null != kmsImpl,
          "KMS Client implementation class is not set (via "
              + EncryptionProperties.ENCRYPTION_KMS_CLIENT_IMPL
              + " catalog property or table property)");

      kmsClient = EncryptionUtil.createKmsClient(kmsImpl);
      kmsClient.initialize(encryptionProperties);
    }

    return kmsClient;
  }

  @Override
  public synchronized void close() throws IOException {
    if (kmsClient != null) {
      kmsClient.close();
      kmsClient = null;
    }
  }
}
