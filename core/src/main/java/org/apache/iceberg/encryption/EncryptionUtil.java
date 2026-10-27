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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.HashWriter;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(EncryptionUtil.class);

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

    if (kmsType != null) {
      kmsImpl =
          switch (kmsType.toLowerCase(Locale.ROOT)) {
            case CatalogProperties.ENCRYPTION_KMS_TYPE_AWS ->
                CatalogProperties.ENCRYPTION_KMS_IMPL_AWS;
            case CatalogProperties.ENCRYPTION_KMS_TYPE_AZURE ->
                CatalogProperties.ENCRYPTION_KMS_IMPL_AZURE;
            case CatalogProperties.ENCRYPTION_KMS_TYPE_GCP ->
                CatalogProperties.ENCRYPTION_KMS_IMPL_GCP;
            default -> throw new IllegalStateException("Unsupported KMS type: " + kmsType);
          };
    }

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

  /**
   * Produces a base64-encoded hash of the serialized table metadata.
   *
   * <p>Catalogs that keep encryption parameters, such as the table key ID, outside of the metadata
   * file can store this hash next to them, in order to detect tampering with a metadata file kept
   * in a storage vulnerable to modification.
   *
   * @param metadata table metadata to hash
   * @return base64-encoded hash of the metadata
   */
  public static String metadataHash(TableMetadata metadata) {
    return Base64.getEncoder().encodeToString(hashOf(metadata));
  }

  /**
   * Verifies that the table metadata loaded from storage matches a hash produced by {@link
   * #metadataHash(TableMetadata)} and kept in a trusted catalog.
   *
   * @param metadata table metadata loaded from storage
   * @param expectedMetadataHash base64-encoded hash retrieved from the catalog
   */
  public static void verifyMetadataHash(TableMetadata metadata, String expectedMetadataHash) {
    byte[] currentHashBytes = hashOf(metadata);
    byte[] expectedHashBytes = Base64.getDecoder().decode(expectedMetadataHash);

    if (!Arrays.equals(expectedHashBytes, currentHashBytes)) {
      throw new RuntimeException(
          String.format(
              "The current metadata file %s might have been modified. Hash of metadata loaded from storage differs "
                  + "from the metadata hash stored in the catalog.",
              metadata.metadataFileLocation()));
    }
  }

  private static byte[] hashOf(TableMetadata tableMetadata) {
    try (HashWriter hashWriter = new HashWriter("SHA-256", StandardCharsets.UTF_8);
        JsonGenerator generator = JsonUtil.factory().createGenerator(hashWriter)) {
      TableMetadataParser.toJson(tableMetadata, generator);
      generator.flush();
      return hashWriter.getHash();
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Unable to produce hash of table metadata", e);
    }
  }

  /**
   * Verifies that table metadata loaded from storage has not been tampered with, using the
   * encryption parameters a catalog recorded for the table.
   *
   * <p>When the catalog recorded a metadata hash, the whole metadata document is verified against
   * it. Otherwise only the encryption parameters are compared, which is weaker, and applies to
   * tables encrypted before catalogs started recording the hash.
   *
   * @param tableName full table name, for logging
   * @param metadata table metadata loaded from storage
   * @param keyIdFromCatalog table key ID recorded by the catalog
   * @param dekLengthFromCatalog data key length recorded by the catalog, may be null
   * @param metadataHashFromCatalog metadata hash recorded by the catalog, may be null
   */
  public static void verifyMetadataIntegrity(
      String tableName,
      TableMetadata metadata,
      String keyIdFromCatalog,
      String dekLengthFromCatalog,
      String metadataHashFromCatalog) {
    if (!Strings.isNullOrEmpty(metadataHashFromCatalog)) {
      verifyMetadataHash(metadata, metadataHashFromCatalog);
      return;
    }

    LOG.warn(
        "Full metadata integrity check skipped because no metadata hash was recorded in the catalog"
            + " for table {}. Falling back to encryption property based check.",
        tableName);

    Map<String, String> propertiesFromMetadata = metadata.properties();

    String keyIdFromMetadata = propertiesFromMetadata.get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (!Objects.equals(keyIdFromCatalog, keyIdFromMetadata)) {
      throw new RuntimeException(
          String.format(
              "Metadata file might have been modified. Encryption key id %s differs from the catalog value %s",
              keyIdFromMetadata, keyIdFromCatalog));
    }

    String dekLengthFromMetadata =
        propertiesFromMetadata.get(TableProperties.ENCRYPTION_DEK_LENGTH);
    if (!Objects.equals(dekLengthFromCatalog, dekLengthFromMetadata)) {
      throw new RuntimeException(
          String.format(
              "Metadata file might have been modified. DEK length %s differs from the catalog value %s",
              dekLengthFromMetadata, dekLengthFromCatalog));
    }
  }

  /**
   * Validates that a commit does not remove or change the table key ID, which must stay the same
   * for the whole lifetime of an encrypted table.
   *
   * @param base table metadata the commit is based on, null when the table is being created
   * @param metadata table metadata being committed
   */
  public static void validateKeyIdUnchanged(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    String baseKeyId = base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    String newKeyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);

    Preconditions.checkArgument(
        baseKeyId == null || newKeyId != null, "Cannot remove key ID from an encrypted table");
    Preconditions.checkArgument(
        Objects.equals(baseKeyId, newKeyId), "Cannot modify key ID of an encrypted table");
  }

  /**
   * Collects the encryption keys that a table needs after a refresh: the keys in committed
   * metadata, plus any keys that are only held by an active encryption manager.
   *
   * <p>Keys of a transaction's uncommitted snapshots are not in committed metadata yet, but the
   * transaction still needs them after the table is refreshed.
   *
   * @param metadata refreshed table metadata
   * @param activeEncryptionManager encryption manager in use before the refresh, may be null
   * @return the merged list of encrypted keys
   */
  public static List<EncryptedKey> mergeEncryptionKeys(
      TableMetadata metadata, EncryptionManager activeEncryptionManager) {
    List<EncryptedKey> keys =
        Optional.ofNullable(metadata.encryptionKeys())
            .map(Lists::newLinkedList)
            .orElseGet(Lists::newLinkedList);

    if (activeEncryptionManager == null) {
      return keys;
    }

    Set<String> keyIdsFromMetadata =
        keys.stream().map(EncryptedKey::keyId).collect(Collectors.toSet());
    for (EncryptedKey key : encryptionKeys(activeEncryptionManager).values()) {
      if (!keyIdsFromMetadata.contains(key.keyId())) {
        keys.add(key);
      }
    }

    return keys;
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
