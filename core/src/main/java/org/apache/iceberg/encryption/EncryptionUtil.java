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
import java.util.Map;
import org.apache.iceberg.BaseManifestListFile;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
   *     KeyManagementClient, long)} instead.
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

    return createEncryptionManager(
        tableKeyId, dataKeyLength, kmsClient, CatalogProperties.WRITER_KEK_TIMEOUT_MS_DEFAULT);
  }

  public static EncryptionManager createEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient, long writerKekTimeout) {
    Preconditions.checkArgument(kmsClient != null, "Invalid KMS client: null");

    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.instance();
    }

    Preconditions.checkState(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);

    return new StandardEncryptionManager(tableKeyId, dataKeyLength, kmsClient, writerKekTimeout);
  }

  public static EncryptedOutputFile plainAsEncryptedOutput(OutputFile encryptingOutputFile) {
    return new BaseEncryptedOutputFile(encryptingOutputFile, EncryptionKeyMetadata.empty());
  }

  public static EncryptionKeyMetadata setFileLength(
      EncryptionKeyMetadata encryptionKeyMetadata, long manifestListLength) {
    Preconditions.checkState(
        encryptionKeyMetadata instanceof StandardKeyMetadata,
        "Cant set file length in %s",
        encryptionKeyMetadata.getClass());
    ((StandardKeyMetadata) encryptionKeyMetadata).setFileLength(manifestListLength);
    return encryptionKeyMetadata;
  }

  private static long parseFileLength(ByteBuffer keyMetadataBuffer) {
    StandardKeyMetadata standardKeyMetadata = StandardKeyMetadata.parse(keyMetadataBuffer);
    return standardKeyMetadata.fileLength();
  }

  public static void getKekCacheFromMetadata(FileIO io, Map<String, KeyEncryptionKey> kekCache) {
    Preconditions.checkState(
        io instanceof EncryptingFileIO,
        "Can't set KEK cache - IO %s is not instance of EncryptingFileIO",
        io.getClass());
    EncryptionManager encryption = ((EncryptingFileIO) io).encryptionManager();
    Preconditions.checkState(
        encryption instanceof StandardEncryptionManager,
        "Can't set KEK cache - encryption manager %s is not instance of StandardEncryptionManager",
        encryption.getClass());
    ((StandardEncryptionManager) encryption).addKekCache(kekCache);
  }

  public static InputFile decryptManifestListFile(
      ManifestListFile manifestListFile, FileIO fileIO) {
    Preconditions.checkArgument(
        fileIO instanceof EncryptingFileIO,
        "Cannot read manifest list (%s) because it is encrypted but the configured "
            + "FileIO (%s) does not implement EncryptingFileIO",
        manifestListFile.location(),
        fileIO.getClass());
    EncryptingFileIO encryptingFileIO = (EncryptingFileIO) fileIO;

    Preconditions.checkArgument(
        encryptingFileIO.encryptionManager() instanceof StandardEncryptionManager,
        "Cannot read manifest list (%s) because it is encrypted but the "
            + "encryption manager (%s) is not StandardEncryptionManager",
        manifestListFile.location(),
        encryptingFileIO.encryptionManager().getClass());
    StandardEncryptionManager standardEncryptionManager =
        (StandardEncryptionManager) encryptingFileIO.encryptionManager();

    KeyEncryptionKey keyEncryptionKey =
        standardEncryptionManager.keyEncryptionKey(manifestListFile.metadataEncryptionKeyID());

    Ciphers.AesGcmDecryptor keyDecryptor = new Ciphers.AesGcmDecryptor(keyEncryptionKey.key());
    ByteBuffer manifestListKeyMetadata =
        ByteBuffer.wrap(
            keyDecryptor.decrypt(
                ByteBuffers.toByteArray(manifestListFile.encryptedKeyMetadata()), null));

    long manifestFileListLength = parseFileLength(manifestListKeyMetadata);

    return encryptingFileIO.newDecryptingInputFile(
        manifestListFile.location(), manifestFileListLength, manifestListKeyMetadata);
  }

  public static ManifestListFile createManifestListFile(
      String location,
      EncryptionManager encryptionManager,
      EncryptionKeyMetadata keyMetadata,
      long length) {
    ByteBuffer manifestListKeyMetadata = null;
    String keyEncryptionKeyID = null;
    ByteBuffer encryptedManifestListKeyMetadata = null;

    // Encrypted manifest list
    if (keyMetadata != null && keyMetadata.buffer() != null) {
      manifestListKeyMetadata = EncryptionUtil.setFileLength(keyMetadata, length).buffer();

      Preconditions.checkState(
          encryptionManager instanceof StandardEncryptionManager,
          "Can't get key encryption key for manifest lists - encryption manager %s is not instance of StandardEncryptionManager",
          encryptionManager.getClass());
      KeyEncryptionKey kek = ((StandardEncryptionManager) encryptionManager).currentKEK();
      Ciphers.AesGcmEncryptor manifestListMDEncryptor = new Ciphers.AesGcmEncryptor(kek.key());

      encryptedManifestListKeyMetadata =
          ByteBuffer.wrap(
              manifestListMDEncryptor.encrypt(
                  ByteBuffers.toByteArray(manifestListKeyMetadata), null));
      keyEncryptionKeyID = kek.id();
    }

    return new BaseManifestListFile(
        location, manifestListKeyMetadata, keyEncryptionKeyID, encryptedManifestListKeyMetadata);
  }

  public static Map<String, KeyEncryptionKey> kekCache(EncryptionManager encryptionManager) {
    Preconditions.checkState(
        encryptionManager instanceof StandardEncryptionManager,
        "Can't get table KEK cache - encryption manager %s is not instance of StandardEncryptionManager",
        encryptionManager.getClass());
    return ((StandardEncryptionManager) encryptionManager).kekCache();
  }
}
