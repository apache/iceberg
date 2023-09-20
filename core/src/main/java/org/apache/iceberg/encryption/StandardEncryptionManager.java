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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.ByteBuffers;

public class StandardEncryptionManager implements EncryptionManager {
  private final transient KeyManagementClient kmsClient;
  private final String tableKeyId;
  private final int dataKeyLength;

  private transient volatile SecureRandom lazyRNG = null;

  /**
   * @param tableKeyId table encryption key id
   * @param dataKeyLength length of data encryption key (16/24/32 bytes)
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   */
  public StandardEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient) {
    Preconditions.checkNotNull(tableKeyId, "Invalid encryption key ID: null");
    Preconditions.checkNotNull(kmsClient, "Invalid KMS client: null");
    this.tableKeyId = tableKeyId;
    this.kmsClient = kmsClient;
    this.dataKeyLength = dataKeyLength;
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    ByteBuffer fileDek = ByteBuffer.allocate(dataKeyLength);
    workerRNG().nextBytes(fileDek.array());

    ByteBuffer aadPrefix = ByteBuffer.allocate(EncryptionProperties.ENCRYPTION_AAD_LENGTH_DEFAULT);
    workerRNG().nextBytes(aadPrefix.array());

    KeyMetadata encryptionMetadata = new KeyMetadata(fileDek, aadPrefix);

    return EncryptedFiles.encryptedOutput(
        new AesGcmOutputFile(rawOutput, fileDek.array(), aadPrefix.array()),
        encryptionMetadata,
        rawOutput);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    KeyMetadata keyMetadata = KeyMetadata.castOrParse(encrypted.keyMetadata());

    byte[] fileDek = ByteBuffers.toByteArray(keyMetadata.encryptionKey());
    byte[] aadPrefix = ByteBuffers.toByteArray(keyMetadata.aadPrefix());

    return new AesGcmInputFile(encrypted.encryptedInputFile(), fileDek, aadPrefix);
  }

  @Override
  public Iterable<InputFile> decrypt(Iterable<EncryptedInputFile> encrypted) {
    // Bulk decrypt is only applied to data files. Returning source input files for parquet.
    return Iterables.transform(encrypted, this::getSourceFile);
  }

  private InputFile getSourceFile(EncryptedInputFile encryptedFile) {
    return encryptedFile.encryptedInputFile();
  }

  private SecureRandom workerRNG() {
    if (this.lazyRNG == null) {
      this.lazyRNG = new SecureRandom();
    }

    return lazyRNG;
  }

  public ByteBuffer wrapKey(ByteBuffer secretKey) {
    if (kmsClient == null) {
      throw new IllegalStateException("Null KmsClient. WrapKey can't be called from workers");
    }

    return kmsClient.wrapKey(secretKey, tableKeyId);
  }

  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    if (kmsClient == null) {
      throw new IllegalStateException("Null KmsClient. UnwrapKey can't be called from workers");
    }

    return kmsClient.unwrapKey(wrappedSecretKey, tableKeyId);
  }
}
