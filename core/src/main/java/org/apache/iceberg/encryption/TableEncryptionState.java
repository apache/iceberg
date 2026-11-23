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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Encryption state of a catalog table, used to create the table's {@link EncryptionManager} and the
 * {@link FileIO} that encrypts and decrypts its files.
 *
 * <p>Catalogs must call {@link #update(String, int, TableMetadata)} whenever they load the catalog
 * entry of an existing table, encrypted or not, with the table key ID and data key length from the
 * catalog rather than from metadata files, which can be tampered with.
 */
public final class TableEncryptionState {
  private final KeyManagementClient kmsClient;
  private boolean loaded = false;
  private String tableKeyId = null;
  private int dataKeyLength = TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
  private List<EncryptedKey> keys = List.of();
  private EncryptionManager encryptionManager = null;
  private EncryptingFileIO encryptingIO = null;

  /**
   * @param kmsClient the catalog's key management client, or null if none is configured
   */
  public TableEncryptionState(KeyManagementClient kmsClient) {
    this.kmsClient = kmsClient;
  }

  /**
   * Updates the state after the catalog loads the table's entry.
   *
   * <p>The metadata is only used for its encryption keys. The table key ID and data key length must
   * come from the catalog.
   *
   * @param newTableKeyId the table key ID from the catalog, or null if the catalog has none
   * @param newDataKeyLength the data encryption key length from the catalog
   * @param metadata the loaded table metadata
   */
  public void update(String newTableKeyId, int newDataKeyLength, TableMetadata metadata) {
    this.loaded = true;
    if (newTableKeyId == null) {
      return;
    }

    Map<String, EncryptedKey> keysById = Maps.newLinkedHashMap();
    for (EncryptedKey key : metadata.encryptionKeys()) {
      keysById.put(key.keyId(), key);
    }

    // keys created by the current encryption manager, for example for snapshots of open
    // transactions, may not be in the loaded metadata yet
    for (EncryptedKey key : knownKeys()) {
      keysById.putIfAbsent(key.keyId(), key);
    }

    this.tableKeyId = newTableKeyId;
    this.dataKeyLength = newDataKeyLength;
    this.keys = List.copyOf(keysById.values());
    this.encryptionManager = null;
    this.encryptingIO = null;
  }

  /**
   * Uses the encryption properties of uncommitted metadata for a table that is being created.
   *
   * <p>Does nothing once the catalog has loaded the table's entry or a table key ID is set.
   *
   * @param uncommittedMetadata uncommitted metadata created by this client, never loaded metadata
   */
  public void adoptForNewTable(TableMetadata uncommittedMetadata) {
    if (loaded || tableKeyId != null) {
      return;
    }

    String newTableKeyId = uncommittedMetadata.property(TableProperties.ENCRYPTION_TABLE_KEY, null);
    if (newTableKeyId != null) {
      this.tableKeyId = newTableKeyId;
      this.dataKeyLength =
          uncommittedMetadata.propertyAsInt(
              TableProperties.ENCRYPTION_DEK_LENGTH, TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);
      this.encryptionManager = null;
      this.encryptingIO = null;
    }
  }

  /** Returns the table's encryption manager, or a plaintext manager if it is not encrypted. */
  public EncryptionManager encryption() {
    if (tableKeyId == null) {
      return PlaintextEncryptionManager.instance();
    }

    if (encryptionManager == null) {
      Preconditions.checkArgument(
          kmsClient != null,
          "Cannot create encryption manager without a key management client. Consider setting the '%s' catalog property",
          CatalogProperties.ENCRYPTION_KMS_IMPL);
      this.encryptionManager =
          EncryptionUtil.createEncryptionManager(
              keys,
              ImmutableMap.of(
                  TableProperties.ENCRYPTION_TABLE_KEY,
                  tableKeyId,
                  TableProperties.ENCRYPTION_DEK_LENGTH,
                  String.valueOf(dataKeyLength)),
              kmsClient);
    }

    return encryptionManager;
  }

  /**
   * Returns a FileIO that encrypts and decrypts the table's files.
   *
   * @param io the catalog's FileIO for the table; callers must pass the same instance every time
   * @return the given FileIO if the table is not encrypted, otherwise an encrypting FileIO
   */
  public FileIO io(FileIO io) {
    if (tableKeyId == null) {
      return io;
    }

    if (encryptingIO == null) {
      this.encryptingIO = EncryptingFileIO.combine(io, encryption());
    }

    return encryptingIO;
  }

  private Iterable<EncryptedKey> knownKeys() {
    return encryptionManager != null
        ? EncryptionUtil.encryptionKeys(encryptionManager).values()
        : keys;
  }
}
