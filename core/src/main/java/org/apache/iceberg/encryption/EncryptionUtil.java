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
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PropertyUtil;

public class EncryptionUtil {

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
      ctor = DynConstructors.builder(KeyManagementClient.class).impl(kmsImpl).buildChecked();
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

  /**
   * @deprecated will be removed in 2.0.0. use {@link #createEncryptionManager(String, int,
   *     KeyManagementClient)} instead.
   */
  @Deprecated
  public static EncryptionManager createEncryptionManager(
      Map<String, String> tableProperties, KeyManagementClient kmsClient) {
    String tableKeyId = tableProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    int dataKeyLength =
        PropertyUtil.propertyAsInt(
            tableProperties,
            TableProperties.ENCRYPTION_DEK_LENGTH,
            TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);

    return createEncryptionManager(tableKeyId, dataKeyLength, kmsClient);
  }

  public static EncryptionManager createEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient) {
    Preconditions.checkArgument(kmsClient != null, "Invalid KMS client: null");

    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.instance();
    }

    Preconditions.checkState(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);

    return new StandardEncryptionManager(tableKeyId, dataKeyLength, ImmutableList.of(), kmsClient);
  }

  public static EncryptedOutputFile plainAsEncryptedOutput(OutputFile encryptingOutputFile) {
    return new BaseEncryptedOutputFile(encryptingOutputFile, EncryptionKeyMetadata.empty());
  }

  /**
   * Decrypt the key metadata for a snapshot.
   *
   * <p>Encryption for snapshot key metadata is only available for tables using standard encryption.
   *
   * @param manifestList a ManifestListFile
   * @param em the table's EncryptionManager
   * @return a decrypted key metadata buffer
   */
  public static ByteBuffer decryptSnapshotKeyMetadata(
      ManifestListFile manifestList, EncryptionManager em) {
    Preconditions.checkState(
        em instanceof StandardEncryptionManager,
        "Snapshot key metadata encryption requires a StandardEncryptionManager");
    ByteBuffer unwrappedKey =
        ((StandardEncryptionManager) em).unwrapKey(manifestList.keyMetadataKeyId());
    return decryptSnapshotKeyMetadata(
        unwrappedKey, manifestList.snapshotId(), manifestList.encryptedKeyMetadata());
  }

  private static ByteBuffer decryptSnapshotKeyMetadata(
      ByteBuffer key, long snapshotId, ByteBuffer encryptedKeyMetadata) {
    Ciphers.AesGcmDecryptor decryptor = new Ciphers.AesGcmDecryptor(ByteBuffers.toByteArray(key));
    byte[] keyMetadataBytes = ByteBuffers.toByteArray(encryptedKeyMetadata);
    byte[] decryptedKeyMetadata = decryptor.decrypt(keyMetadataBytes, snapshotIdAsAAD(snapshotId));
    return ByteBuffer.wrap(decryptedKeyMetadata);
  }

  /**
   * Encrypts the key metadata for a snapshot.
   *
   * <p>Encryption for snapshot key metadata is only available for tables using standard encryption.
   *
   * @param key unwrapped snapshot key bytes
   * @param snapshotId ID of the table snapshot
   * @param keyMetadata unencrypted EncryptionKeyMetadata
   * @return a Pair of the key ID used to encrypt and the encrypted key metadata
   */
  public static ByteBuffer encryptSnapshotKeyMetadata(
      ByteBuffer key, long snapshotId, EncryptionKeyMetadata keyMetadata) {
    Ciphers.AesGcmEncryptor encryptor = new Ciphers.AesGcmEncryptor(ByteBuffers.toByteArray(key));
    byte[] keyMetadataBytes = ByteBuffers.toByteArray(keyMetadata.buffer());
    byte[] encryptedKeyMetadata = encryptor.encrypt(keyMetadataBytes, snapshotIdAsAAD(snapshotId));
    return ByteBuffer.wrap(encryptedKeyMetadata);
  }

  private static byte[] snapshotIdAsAAD(long snapshotId) {
    ByteBuffer asBuffer =
        ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, snapshotId);
    return ByteBuffers.toByteArray(asBuffer);
  }
}
