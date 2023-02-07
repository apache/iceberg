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
package org.apache.iceberg.encryption.envelope;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Map;
import org.apache.iceberg.encryption.BaseEncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Encryption manager which in conjunction with a KMS can encrypt {@link OutputFile} and decrypt
 * {@link InputFile}. Envelope encryption uses a key wrapping strategy, where a Key Encryption Key
 * (KEK) is used to wrap or unwrap a Data Encryption Key DEK which is used to encrypt the underlying
 * files.
 *
 * <p>When generating new DEKs for OutputFiles, this class will first attempt to have the KMS
 * generate a new key. If the KMS does not support key generation a new key will be produced by
 * pulling bytes from a {@link SecureRandom} on the JVM writing the file.
 */
public class EnvelopeEncryptionManager implements EncryptionManager {
  public static final String ENCRYPTION_TABLE_KEY = "encryption.table.key.id";

  public static final String ENCRYPTION_DEK_LENGTH = "encryption.data.key.length";
  public static final int ENCRYPTION_DEK_LENGTH_DEFAULT = 16;

  public static final int ENCRYPTION_AAD_LENGTH_DEFAULT = 16;

  /** Implementation of the KMS client for envelope encryption */
  public static final String ENCRYPTION_KMS_CLIENT_IMPL = "encryption.kms.client-impl";

  private final KmsClient kmsClient;
  private String tableKeyId;
  private int dataKeyLength;
  private boolean kmsGeneratedKeys;

  private transient volatile SecureRandom workerRNG = null;

  /**
   * @param tableKeyId table encryption key id
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   * @param encryptionProperties encryption properties
   */
  public EnvelopeEncryptionManager(
      String tableKeyId, KmsClient kmsClient, Map<String, String> encryptionProperties) {
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
            encryptionProperties, ENCRYPTION_DEK_LENGTH, ENCRYPTION_DEK_LENGTH_DEFAULT);
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    if (null == workerRNG) {
      workerRNG = new SecureRandom();
    }

    ByteBuffer fileDek = ByteBuffer.allocate(dataKeyLength);
    workerRNG.nextBytes(fileDek.array());

    ByteBuffer aadPrefix = ByteBuffer.allocate(ENCRYPTION_AAD_LENGTH_DEFAULT);
    workerRNG.nextBytes(aadPrefix.array());

    EnvelopeKeyMetadata fileEnvelopeMetadata = new EnvelopeKeyMetadata(null, fileDek, aadPrefix);

    return new BaseEncryptedOutputFile(rawOutput, fileEnvelopeMetadata);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (encrypted.keyMetadata().buffer() == null) { // unencrypted file
      throw new RuntimeException(
          "Unencrypted file " + encrypted.encryptedInputFile().location() + " in encrypted table");
    }
    return encrypted.encryptedInputFile();
  }
}
