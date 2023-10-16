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
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;

public class EncryptionUtil {

  private EncryptionUtil() {}

  public static KeyMetadata parseKeyMetadata(ByteBuffer metadataBuffer) {
    return KeyMetadata.parse(metadataBuffer);
  }

  public static EncryptionKeyMetadata createKeyMetadata(ByteBuffer key, ByteBuffer aadPrefix) {
    return new KeyMetadata(key, aadPrefix);
  }

  public static long gcmEncryptionLength(long plainFileLength) {
    int numberOfFullBlocks = Math.toIntExact(plainFileLength / Ciphers.PLAIN_BLOCK_SIZE);
    int plainBytesInLastBlock =
        Math.toIntExact(plainFileLength - numberOfFullBlocks * Ciphers.PLAIN_BLOCK_SIZE);
    boolean fullBlocksOnly = (0 == plainBytesInLastBlock);
    int cipherBytesInLastBlock =
        fullBlocksOnly ? 0 : plainBytesInLastBlock + Ciphers.NONCE_LENGTH + Ciphers.GCM_TAG_LENGTH;
    int cipherBlockSize = Ciphers.PLAIN_BLOCK_SIZE + Ciphers.NONCE_LENGTH + Ciphers.GCM_TAG_LENGTH;
    return (long) Ciphers.GCM_STREAM_HEADER_LENGTH
        + numberOfFullBlocks * cipherBlockSize
        + cipherBytesInLastBlock;
  }

  public static KeyManagementClient createKmsClient(Map<String, String> catalogProperties) {
    String kmsType = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_TYPE);

    if (kmsType == null) {
      throw new IllegalStateException(
          "Cannot create StandardEncryptionManagerFactory without KMS type");
    }

    if (!kmsType.equals(CatalogProperties.ENCRYPTION_KMS_CUSTOM_TYPE)) {
      // Currently support only custom types
      throw new UnsupportedOperationException("Undefined KMS type " + kmsType);
    }

    String kmsClientImpl = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL);

    if (kmsClientImpl == null) {
      throw new IllegalStateException("Custom KMS client class is not defined");
    }

    KeyManagementClient kmsClient;
    DynConstructors.Ctor<KeyManagementClient> ctor;
    try {
      ctor = DynConstructors.builder(KeyManagementClient.class).impl(kmsClientImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize KeyManagementClient, missing no-arg constructor for class %s",
              kmsClientImpl),
          e);
    }

    try {
      kmsClient = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize kms client, %s does not implement KeyManagementClient interface",
              kmsClientImpl),
          e);
    }

    kmsClient.initialize(catalogProperties);

    return kmsClient;
  }

  public static EncryptionManager createEncryptionManager(
      Map<String, String> tableProperties, KeyManagementClient kmsClient) {
    String tableKeyId = tableProperties.get(ENCRYPTION_TABLE_KEY);

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
            tableProperties, ENCRYPTION_DEK_LENGTH, ENCRYPTION_DEK_LENGTH_DEFAULT);

    return new StandardEncryptionManager(tableKeyId, dataKeyLength, kmsClient);
  }

  public static boolean useNativeEncryption(EncryptionKeyMetadata keyMetadata) {
    return keyMetadata != null && keyMetadata instanceof KeyMetadata;
  }
}
