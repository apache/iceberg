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
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

public class StandardEncryptionManager implements EncryptionManager {
  private final KeyManagementClient kmsClient;
  private String tableKeyId;
  private int dataKeyLength;
  private boolean kmsGeneratedKeys;

  private transient volatile SecureRandom workerRNG = null;

  /**
   * @param tableKeyId table encryption key id
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   * @param encryptionProperties encryption properties
   */
  public StandardEncryptionManager(
      String tableKeyId, KeyManagementClient kmsClient, Map<String, String> encryptionProperties) {
    Preconditions.checkNotNull(
        tableKeyId,
        "Cannot create EnvelopeEncryptionManager because table encryption key ID is not specified");
    Preconditions.checkNotNull(
        kmsClient, "Cannot create EnvelopeEncryptionManager because kmsClient is null");
    Preconditions.checkNotNull(
        encryptionProperties,
        "Cannot create EnvelopeEncryptionManager because encryptionProperties are not passed");
    this.tableKeyId = tableKeyId;
    this.kmsClient = kmsClient;
    this.kmsGeneratedKeys = kmsClient.supportsKeyGeneration();

    this.dataKeyLength =
        PropertyUtil.propertyAsInt(
            encryptionProperties,
            EncryptionProperties.ENCRYPTION_DEK_LENGTH,
            EncryptionProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    if (null == workerRNG) {
      createSecureRandomGenerator();
    }

    ByteBuffer fileDek = ByteBuffer.allocate(dataKeyLength);
    workerRNG.nextBytes(fileDek.array());

    ByteBuffer aadPrefix = ByteBuffer.allocate(EncryptionProperties.ENCRYPTION_AAD_LENGTH_DEFAULT);
    workerRNG.nextBytes(aadPrefix.array());

    // For data files
    KeyMetadata dataEncryptionMetadata = new KeyMetadata(fileDek, null, aadPrefix);

    // For metadata files
    // This is an expensive operation, RPC to KMS server
    ByteBuffer wrappedDek = kmsClient.wrapKey(fileDek, tableKeyId);
    KeyMetadata manifestEncryptionMetadata = new KeyMetadata(wrappedDek, tableKeyId, aadPrefix);

    // We don't know which key metadata to use, because we don't know what file we encrypt.
    // Works for data files:
    // return new BaseEncryptedOutputFile(new AesGcmOutputFile(rawOutput, fileDek, aadPrefix),
    //    dataEncryptionMetadata, rawOutput);
    // Works for manifest files:
    // return new BaseEncryptedOutputFile(new AesGcmOutputFile(rawOutput, fileDek, aadPrefix),
    //    manifestEncryptionMetadata, rawOutput);

    // Temp return, for parquet data only. TODO - remove
    return new BaseEncryptedOutputFile(null, dataEncryptionMetadata, rawOutput);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (encrypted.keyMetadata() == null || encrypted.keyMetadata().buffer() == null) {
      throw new RuntimeException(
          "Unencrypted file " + encrypted.encryptedInputFile().location() + " in encrypted table");
    }

    KeyMetadata keyMetadata = KeyMetadata.parse(encrypted.keyMetadata().buffer());

    ByteBuffer fileDek;
    if (keyMetadata.wrappingKeyId() == null) {
      fileDek = keyMetadata.encryptionKey();
    } else {
      fileDek = kmsClient.unwrapKey(keyMetadata.encryptionKey(), keyMetadata.wrappingKeyId());
    }

    // return new AesGcmInputFile(encrypted.encryptedInputFile(), fileDek, keyMetadata.aadPrefix());

    // Temp return null - decrypt is not called for parquet data files. TODO - remove
    return null;
  }

  private void createSecureRandomGenerator() {
    this.workerRNG = new SecureRandom();
  }
}
