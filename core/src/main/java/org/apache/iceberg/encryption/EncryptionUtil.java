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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PropertyUtil;

public class EncryptionUtil {
  private static final Set<String> ENCRYPTION_TABLE_PROPERTIES =
      ImmutableSet.<String>builder()
          .add(TableProperties.ENCRYPTION_TABLE_KEY)
          .add(TableProperties.ENCRYPTION_DEK_LENGTH)
          .build();

  private EncryptionUtil() {}

  public static KeyManagementClient createKmsClient(Map<String, String> catalogProperties) {
    String kmsType = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_TYPE);
    String kmsImpl = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_IMPL);

    Preconditions.checkArgument(
        kmsType == null || kmsImpl == null,
        "Cannot set both KMS type (%s) and KMS impl (%s)",
        kmsType,
        kmsImpl);

    // TODO: Add KMS implementations
    Preconditions.checkArgument(kmsType == null, "Unsupported KMS type: %s", kmsType);

    KeyManagementClient kmsClient;
    DynConstructors.Ctor<KeyManagementClient> ctor;
    try {
      ctor =
          DynConstructors.builder(KeyManagementClient.class)
              .loader(EncryptionUtil.class.getClassLoader())
              .impl(kmsImpl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize KeyManagementClient, missing no-arg constructor for class %s",
              kmsImpl),
          e);
    }

    try {
      kmsClient = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize kms client, %s does not implement KeyManagementClient interface",
              kmsImpl),
          e);
    }

    kmsClient.initialize(catalogProperties);

    return kmsClient;
  }

  public static EncryptionManager createEncryptionManager(
      List<EncryptedKey> keys, Map<String, String> tableProperties, KeyManagementClient kmsClient) {
    Preconditions.checkArgument(kmsClient != null, "Invalid KMS client: null");
    String tableKeyId = tableProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);

    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.instance();
    }

    int dataKeyLength =
        PropertyUtil.propertyAsInt(
            tableProperties,
            TableProperties.ENCRYPTION_DEK_LENGTH,
            TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);

    Preconditions.checkState(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);

    return new StandardEncryptionManager(keys, tableKeyId, dataKeyLength, kmsClient);
  }

  public static EncryptedOutputFile plainAsEncryptedOutput(OutputFile encryptingOutputFile) {
    return new BaseEncryptedOutputFile(encryptingOutputFile, EncryptionKeyMetadata.empty());
  }

  public static ByteBuffer setFileLength(ByteBuffer keyMetadata, long fileLength) {
    if (keyMetadata == null) {
      return null;
    }

    return StandardKeyMetadata.parse(keyMetadata).copyWithLength(fileLength).buffer();
  }

  /**
   * Decrypt the key metadata for a manifest list.
   *
   * @param manifestList a ManifestListFile
   * @param em the table's EncryptionManager
   * @return a decrypted key metadata buffer
   */
  public static ByteBuffer decryptManifestListKeyMetadata(
      ManifestListFile manifestList, EncryptionManager em) {
    Preconditions.checkState(
        em instanceof StandardEncryptionManager,
        "Snapshot key metadata encryption requires a StandardEncryptionManager");
    StandardEncryptionManager sem = (StandardEncryptionManager) em;
    String manifestListKeyId = manifestList.encryptionKeyID();
    Map<String, EncryptedKey> encryptionKeys = sem.encryptionKeys();
    EncryptedKey manifestListKey = encryptionKeys.get(manifestListKeyId);
    ByteBuffer encryptedKeyMetadata = manifestListKey.encryptedKeyMetadata();
    String keyEncryptionKeyID = manifestListKey.encryptedById();
    ByteBuffer keyEncryptionKey = sem.encryptedByKey(manifestListKeyId);
    String keyEncryptionKeyTimestamp =
        encryptionKeys
            .get(keyEncryptionKeyID)
            .properties()
            .get(StandardEncryptionManager.KEY_TIMESTAMP);
    Preconditions.checkState(
        keyEncryptionKeyTimestamp != null, "Key encryption key must be timestamped");
    Ciphers.AesGcmDecryptor decryptor =
        new Ciphers.AesGcmDecryptor(ByteBuffers.toByteArray(keyEncryptionKey));
    byte[] keyMetadataBytes = ByteBuffers.toByteArray(encryptedKeyMetadata);

    // Use key encryption key timestamp as AES GCM signature (AAD) of encryption - in order to
    // prevent timestamp tampering attacks
    byte[] decryptedKeyMetadata =
        decryptor.decrypt(
            keyMetadataBytes, keyEncryptionKeyTimestamp.getBytes(StandardCharsets.UTF_8));

    return ByteBuffer.wrap(decryptedKeyMetadata);
  }

  public static Map<String, EncryptedKey> encryptionKeys(EncryptionManager em) {
    Preconditions.checkState(
        em instanceof StandardEncryptionManager,
        "Retrieving encryption keys requires a StandardEncryptionManager");
    StandardEncryptionManager sem = (StandardEncryptionManager) em;
    return sem.encryptionKeys();
  }

  /**
   * Encrypts the key metadata for a manifest list.
   *
   * @param key key encryption key bytes
   * @param keyTimestamp timestamp of the key encryption key
   * @param mlkMetadata manifest list key metadata
   * @return encrypted key metadata
   */
  static ByteBuffer encryptManifestListKeyMetadata(
      ByteBuffer key, String keyTimestamp, EncryptionKeyMetadata mlkMetadata) {
    Ciphers.AesGcmEncryptor encryptor = new Ciphers.AesGcmEncryptor(ByteBuffers.toByteArray(key));
    byte[] mlkMetadataBytes = ByteBuffers.toByteArray(mlkMetadata.buffer());

    // Use key encryption key timestamp as AES GCM signature (AAD) of encryption - in order to
    // prevent timestamp tampering attacks
    byte[] encryptedKeyMetadata =
        encryptor.encrypt(mlkMetadataBytes, keyTimestamp.getBytes(StandardCharsets.UTF_8));

    return ByteBuffer.wrap(encryptedKeyMetadata);
  }

  public static void checkCompatibility(Map<String, String> tableProperties, int formatVersion) {
    if (formatVersion >= 3) {
      return;
    }

    Set<String> encryptionProperties =
        Sets.intersection(ENCRYPTION_TABLE_PROPERTIES, tableProperties.keySet());
    Preconditions.checkArgument(
        encryptionProperties.isEmpty(),
        "Invalid properties for v%s: %s",
        formatVersion,
        encryptionProperties);
  }
}
