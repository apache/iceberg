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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.util.PropertyUtil;

public class StandardEncryptionManagerFactory implements Closeable {
  private final KeyManagementClient kmsClient;

  public StandardEncryptionManagerFactory(Map<String, String> catalogProperties) {
    String kmsType = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_TYPE);

    if (kmsType == null) {
      throw new IllegalStateException("Cannot create StandardEncryptionManagerFactory without KMS type");
    }

    if (kmsType.equals(CatalogProperties.ENCRYPTION_KMS_CUSTOM_TYPE)) {
      String kmsClientImpl = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL);
      if (kmsClientImpl == null) {
        throw new IllegalStateException("Custom KMS client class is not defined");
      }
      kmsClient = EncryptionUtil.createKmsClient(kmsClientImpl);
      kmsClient.initialize(catalogProperties);
    } else {
      // Currently support only custom types
      throw new UnsupportedOperationException("Undefined KMS type " + kmsType);
    }
  }

  public EncryptionManager create(Map<String, String> tableProperties) {
    String tableKeyId = tableProperties.get(EncryptionProperties.ENCRYPTION_TABLE_KEY);

    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.instance();
    }

    if (kmsClient == null) {
      throw new IllegalStateException("Encrypted table. No KMS client is configured in catalog");
    }

    String fileFormat =
        PropertyUtil.propertyAsString(
            tableProperties, DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);

    if (FileFormat.fromString(fileFormat) != FileFormat.PARQUET) {
      throw new UnsupportedOperationException(
          "Iceberg encryption currently supports only parquet format for data files");
    }

    int dataKeyLength =
        PropertyUtil.propertyAsInt(
            tableProperties,
            EncryptionProperties.ENCRYPTION_DEK_LENGTH,
            EncryptionProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);

    return new StandardEncryptionManager(tableKeyId, dataKeyLength, kmsClient);
  }

  @Override
  public synchronized void close() throws IOException {
    if (kmsClient != null) {
      kmsClient.close();
    }
  }
}
