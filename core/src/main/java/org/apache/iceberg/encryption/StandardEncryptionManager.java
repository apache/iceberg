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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
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
    Preconditions.checkArgument(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);
    Preconditions.checkNotNull(kmsClient, "Invalid KMS client: null");
    this.tableKeyId = tableKeyId;
    this.kmsClient = kmsClient;
    this.dataKeyLength = dataKeyLength;
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile plainOutput) {
    return new StandardEncryptedOutputFile(plainOutput, dataKeyLength);
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    // this input file will lazily parse key metadata in case the file is not an AES GCM stream.
    return new StandardDecryptedInputFile(encrypted);
  }

  @Override
  public Iterable<InputFile> decrypt(Iterable<EncryptedInputFile> encrypted) {
    // Bulk decrypt is only applied to data files. Returning source input files for parquet.
    return Iterables.transform(encrypted, this::decrypt);
  }

  private SecureRandom workerRNG() {
    if (this.lazyRNG == null) {
      this.lazyRNG = new SecureRandom();
    }

    return lazyRNG;
  }

  public ByteBuffer wrapKey(ByteBuffer secretKey) {
    if (kmsClient == null) {
      throw new IllegalStateException(
          "Cannot wrap key after called after serialization (missing KMS client)");
    }

    return kmsClient.wrapKey(secretKey, tableKeyId);
  }

  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    if (kmsClient == null) {
      throw new IllegalStateException(
          "Cannot wrap key after called after serialization (missing KMS client)");
    }

    return kmsClient.unwrapKey(wrappedSecretKey, tableKeyId);
  }

  private class StandardEncryptedOutputFile implements EncryptedOutputFile {
    private final OutputFile plainOutputFile;
    private final int dataKeyLength;
    private StandardKeyMetadata lazyKeyMetadata = null;
    private OutputFile lazyEncryptingOutputFile = null;

    StandardEncryptedOutputFile(OutputFile plainOutputFile, int dataKeyLength) {
      this.plainOutputFile = plainOutputFile;
      this.dataKeyLength = dataKeyLength;
    }

    @Override
    public StandardKeyMetadata keyMetadata() {
      if (null == lazyKeyMetadata) {
        byte[] fileDek = new byte[dataKeyLength];
        workerRNG().nextBytes(fileDek);

        byte[] aadPrefix = new byte[TableProperties.ENCRYPTION_AAD_LENGTH_DEFAULT];
        workerRNG().nextBytes(aadPrefix);

        this.lazyKeyMetadata = new StandardKeyMetadata(fileDek, aadPrefix);
      }

      return lazyKeyMetadata;
    }

    @Override
    public OutputFile encryptingOutputFile() {
      if (null == lazyEncryptingOutputFile) {
        this.lazyEncryptingOutputFile =
            new AesGcmOutputFile(
                plainOutputFile,
                ByteBuffers.toByteArray(keyMetadata().encryptionKey()),
                ByteBuffers.toByteArray(keyMetadata().aadPrefix()));
      }

      return lazyEncryptingOutputFile;
    }

    @Override
    public OutputFile plainOutputFile() {
      return plainOutputFile;
    }
  }

  private static class StandardDecryptedInputFile implements InputFile {
    private final EncryptedInputFile encryptedInputFile;
    private StandardKeyMetadata lazyKeyMetadata = null;
    private AesGcmInputFile lazyDecryptedInputFile = null;

    private StandardDecryptedInputFile(EncryptedInputFile encryptedInputFile) {
      this.encryptedInputFile = encryptedInputFile;
    }

    private StandardKeyMetadata keyMetadata() {
      if (null == lazyKeyMetadata) {
        this.lazyKeyMetadata = StandardKeyMetadata.castOrParse(encryptedInputFile.keyMetadata());
      }

      return lazyKeyMetadata;
    }

    private AesGcmInputFile decrypted() {
      if (null == lazyDecryptedInputFile) {
        this.lazyDecryptedInputFile =
            new AesGcmInputFile(
                encryptedInputFile.encryptedInputFile(),
                ByteBuffers.toByteArray(keyMetadata().encryptionKey()),
                ByteBuffers.toByteArray(keyMetadata().aadPrefix()));
      }

      return lazyDecryptedInputFile;
    }

    @Override
    public long getLength() {
      return decrypted().getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return decrypted().newStream();
    }

    @Override
    public String location() {
      return decrypted().location();
    }

    @Override
    public boolean exists() {
      return decrypted().exists();
    }
  }
}
