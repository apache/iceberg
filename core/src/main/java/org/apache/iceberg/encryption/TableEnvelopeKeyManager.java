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
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class TableEnvelopeKeyManager implements EnvelopeKeyManager {
  private final KmsClient kmsClient;
  private final String tableKekID;
  private final boolean doubleWrap;

  private final boolean uniformEncryption;
  private Set<String> columnNamesUniformEncryption;
  private final int dataKeyLength;

  private transient volatile SecureRandom workerRNG = null;

  public TableEnvelopeKeyManager(KmsClient kmsClient, EnvelopeConfig dataEnvelopeConfig, boolean pushdown,
                                 Schema schema, int dataKeyLength) {
    this.kmsClient = kmsClient;
    this.dataKeyLength = dataKeyLength;

    if (dataEnvelopeConfig.isDoubleEnvelope()) {
      throw new IllegalArgumentException("Double envelope encryption is not supported yet");
    } else { // single wrapping
      doubleWrap = false;
      tableKekID = dataEnvelopeConfig.kekId();
      Preconditions.checkArgument(tableKekID != null,
              "Table key encryption key ID is not configured");
    }

    if (pushdown && dataEnvelopeConfig.columnConfigMap().isEmpty() && dataEnvelopeConfig.columnConfigs().isEmpty()) {
      uniformEncryption = true;
      // List all columns in table. So a DEK will be generated for each, in uniform encryption.
      columnNamesUniformEncryption = new HashSet<>();
      for (Types.NestedField column : schema.columns()) {
        columnNamesUniformEncryption.add(column.name());
      }
    } else {
      uniformEncryption = false;
    }
  }

  @Override
  public KmsClient kmsClient() {
    return kmsClient;
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public EnvelopeMetadata generate(EnvelopeConfig config) {
    if (null == workerRNG) {
      workerRNG = new SecureRandom();
    }

    if (doubleWrap) {
      throw new RuntimeException("Double envelope encryption is not supported yet");
    } else { // single wrap
      if (!tableKekID.equals(config.kekId())) {
        throw new RuntimeException("Wrong table KEK ID: " + config.kekId() + " instead of " + tableKekID);
      }

      byte[] fileDek;
      String wrappedFileDEK;
      if (kmsClient.supportsKeyGeneration()) {
        // TODO is context needed?
        KmsClient.KeyGenerationResult generatedDek = kmsClient.generateKey(tableKekID);
        fileDek = generatedDek.key().array();
        wrappedFileDEK = generatedDek.wrappedKey();
      } else {
        fileDek = new byte[dataKeyLength];
        workerRNG.nextBytes(fileDek);
        // TODO is context needed?
        wrappedFileDEK = kmsClient.wrapKey(ByteBuffer.wrap(fileDek), tableKekID);
      }

      // TODO skip for metadata files and lists. Use EnvelopeConfig.properties?
      Set<EnvelopeMetadata> columnMetadataSet = null;
      if (uniformEncryption) {
        columnMetadataSet = new HashSet<>();
        for (String columnName : columnNamesUniformEncryption) {
          byte[] columnDek;
          String wrappedColumnDek;
          if (kmsClient.supportsKeyGeneration()) {
            KmsClient.KeyGenerationResult generatedDek = kmsClient.generateKey(tableKekID);
            columnDek = generatedDek.key().array();
            wrappedColumnDek = generatedDek.wrappedKey();
          } else {
            columnDek = new byte[dataKeyLength];
            workerRNG.nextBytes(columnDek);
            wrappedColumnDek = kmsClient.wrapKey(ByteBuffer.wrap(columnDek), tableKekID);
          }
          EnvelopeMetadata columnEnvelopeMetadata = new EnvelopeMetadata(null, tableKekID, null,
                  wrappedColumnDek, null, null, null, columnName, null,
                  null);
          columnEnvelopeMetadata.setDek(ByteBuffer.wrap(columnDek));
          columnMetadataSet.add(columnEnvelopeMetadata);
        }
      } else if (!config.columnConfigs().isEmpty()) { // Column keys
        throw new RuntimeException("Per-column MEKs/KEKs not supported yet");
      }

      EnvelopeMetadata fileEnvelopeMetadata = new EnvelopeMetadata(null, tableKekID, null,
              wrappedFileDEK, null, config.algorithm(), null, null, null,
              columnMetadataSet);
      fileEnvelopeMetadata.setDek(ByteBuffer.wrap(fileDek));
      return fileEnvelopeMetadata;
    }
  }

  @Override
  public void decrypt(EnvelopeMetadata encryptedMetadata) {
    if (doubleWrap) {
      throw new RuntimeException("Double envelope encryption is not supported yet");
    } else { // single wrap
      if (!tableKekID.equals(encryptedMetadata.kekId())) {
        throw new RuntimeException("Wrong table KEK ID: " + encryptedMetadata.kekId() + " instead of " + tableKekID);
      }
      // TODO is context needed?
      ByteBuffer fileDek = kmsClient.unwrapKey(encryptedMetadata.wrappedDek(), tableKekID);
      encryptedMetadata.setDek(fileDek);

      // TODO for column-specific MEKs/KEKs, unwrap only projected columns
      for (EnvelopeMetadata column : encryptedMetadata.columnMetadata()) {
        ByteBuffer columnDek = kmsClient.unwrapKey(column.wrappedDek(), column.kekId());
        column.setDek(columnDek);
      }
    }
  }

  @Override
  public boolean doubleEnvelope() {
    return doubleWrap;
  }

  public static KmsClient loadKmsClient(String classPath, Map<String, String> properties) {
    if (null == classPath) {
      throw new IllegalArgumentException("Cannot initialize KmsClient, null class name");
    }

    DynConstructors.Ctor<KmsClient> ctor;
    try {
      ctor = DynConstructors.builder(KmsClient.class).impl(classPath).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
              "Cannot initialize KmsClient, missing no-arg constructor: %s", classPath), e);
    }

    KmsClient kmsClient;
    try {
      kmsClient = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
              String.format("Cannot initialize KmsClient, %s does not implement KmsClient.", classPath), e);
    }

    kmsClient.initialize(properties);
    return kmsClient;
  }
}
