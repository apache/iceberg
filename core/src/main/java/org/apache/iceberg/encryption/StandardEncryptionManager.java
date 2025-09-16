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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

public class StandardEncryptionManager implements EncryptionManager {
  private static final String KEY_ENCRYPTION_KEY_ID = "KEY_ENCRYPTION_KEY_ID";

  private final String tableKeyId;
  private final int dataKeyLength;

  // unserializable elements of the EncryptionManager
  private class TransientEncryptionState {
    private final KeyManagementClient kmsClient;
    private final Map<String, EncryptedKey> encryptionKeys;
    private final LoadingCache<String, ByteBuffer> unwrappedKeyCache;

    private TransientEncryptionState(KeyManagementClient kmsClient) {
      this.kmsClient = kmsClient;
      this.encryptionKeys = Maps.newLinkedHashMap();
      this.unwrappedKeyCache =
          Caffeine.newBuilder()
              .expireAfterWrite(1, TimeUnit.HOURS)
              .build(
                  keyId ->
                      kmsClient.unwrapKey(
                          encryptionKeys.get(keyId).encryptedKeyMetadata(), tableKeyId));
    }
  }

  private final transient TransientEncryptionState transientState;

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
    this.transientState = new TransientEncryptionState(kmsClient);
    this.dataKeyLength = dataKeyLength;
  }

  @Override
  public NativeEncryptionOutputFile encrypt(OutputFile plainOutput) {
    return new StandardEncryptedOutputFile(plainOutput, dataKeyLength);
  }

  @Override
  public NativeEncryptionInputFile decrypt(EncryptedInputFile encrypted) {
    // this input file will lazily parse key metadata in case the file is not an AES GCM stream.
    if (encrypted instanceof NativeEncryptionInputFile) {
      return (NativeEncryptionInputFile) encrypted;
    }

    return new StandardDecryptedInputFile(encrypted);
  }

  @Override
  public Iterable<InputFile> decrypt(Iterable<EncryptedInputFile> encrypted) {
    return Iterables.transform(encrypted, this::decrypt);
  }

  private SecureRandom workerRNG() {
    if (this.lazyRNG == null) {
      this.lazyRNG = new SecureRandom();
    }

    return lazyRNG;
  }

  /**
   * @deprecated will be removed in 2.0.
   */
  @Deprecated
  public ByteBuffer wrapKey(ByteBuffer secretKey) {
    if (transientState == null) {
      throw new IllegalStateException(
          "Cannot wrap key after called after serialization (missing KMS client)");
    }

    return transientState.kmsClient.wrapKey(secretKey, tableKeyId);
  }

  /**
   * @deprecated will be removed in 2.0.
   */
  @Deprecated
  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    if (transientState == null) {
      throw new IllegalStateException("Cannot unwrap key after serialization");
    }

    return transientState.kmsClient.unwrapKey(wrappedSecretKey, tableKeyId);
  }

  private String keyEncryptionKeyID() {
    if (transientState == null) {
      throw new IllegalStateException("Cannot return the current key after serialization");
    }

    if (!transientState.encryptionKeys.containsKey(KEY_ENCRYPTION_KEY_ID)) {
      ByteBuffer unwrapped = newKey();
      ByteBuffer wrapped = transientState.kmsClient.wrapKey(unwrapped, tableKeyId);
      EncryptedKey key = new BaseEncryptedKey(KEY_ENCRYPTION_KEY_ID, wrapped, tableKeyId, null);

      // update internal tracking
      transientState.unwrappedKeyCache.put(key.keyId(), unwrapped);
      transientState.encryptionKeys.put(key.keyId(), key);
    }

    return KEY_ENCRYPTION_KEY_ID;
  }

  ByteBuffer encryptedByKey(String manifestListKeyID) {
    if (transientState == null) {
      throw new IllegalStateException("Cannot find key encryption key after serialization");
    }

    EncryptedKey encryptedKeyMetadata = transientState.encryptionKeys.get(manifestListKeyID);
    if (encryptedKeyMetadata == null) {
      throw new IllegalStateException(
          "Cannot find manifest list key metadata with id " + manifestListKeyID);
    }

    return transientState.unwrappedKeyCache.get(encryptedKeyMetadata.encryptedById());
  }

  ByteBuffer encryptedKeyMetadata(String manifestListKeyID) {
    if (transientState == null) {
      throw new IllegalStateException("Cannot find encrypted key metadata after serialization");
    }

    EncryptedKey encryptedKeyMetadata = transientState.encryptionKeys.get(manifestListKeyID);
    if (encryptedKeyMetadata == null) {
      throw new IllegalStateException(
          "Cannot find manifest list key metadata with id " + manifestListKeyID);
    }

    return encryptedKeyMetadata.encryptedKeyMetadata();
  }

  public String addManifestListKeyMetadata(NativeEncryptionKeyMetadata keyMetadata) {
    if (transientState == null) {
      throw new IllegalStateException("Cannot add key metadata after serialization");
    }

    String manifestListKeyID = generateKeyId();
    ByteBuffer encryptedKeyMetadata =
        EncryptionUtil.encryptManifestListKeyMetadata(
            transientState.unwrappedKeyCache.get(keyEncryptionKeyID()),
            manifestListKeyID,
            keyMetadata);
    BaseEncryptedKey key =
        new BaseEncryptedKey(manifestListKeyID, encryptedKeyMetadata, keyEncryptionKeyID(), null);

    transientState.encryptionKeys.put(key.keyId(), key);

    return manifestListKeyID;
  }

  private String generateKeyId() {
    byte[] idBytes = new byte[16];
    workerRNG().nextBytes(idBytes);
    return Base64.getEncoder().encodeToString(idBytes);
  }

  private ByteBuffer newKey() {
    byte[] newKey = new byte[dataKeyLength];
    workerRNG().nextBytes(newKey);
    return ByteBuffer.wrap(newKey);
  }

  private class StandardEncryptedOutputFile implements NativeEncryptionOutputFile {
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

  private static class StandardDecryptedInputFile implements NativeEncryptionInputFile {
    private final EncryptedInputFile encryptedInputFile;
    private StandardKeyMetadata lazyKeyMetadata = null;
    private AesGcmInputFile lazyDecryptedInputFile = null;

    private StandardDecryptedInputFile(EncryptedInputFile encryptedInputFile) {
      this.encryptedInputFile = encryptedInputFile;
    }

    @Override
    public InputFile encryptedInputFile() {
      return encryptedInputFile.encryptedInputFile();
    }

    @Override
    public StandardKeyMetadata keyMetadata() {
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
                ByteBuffers.toByteArray(keyMetadata().aadPrefix()),
                keyMetadata().fileLength());
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
