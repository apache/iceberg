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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

public class StandardEncryptionManager implements EncryptionManager {
  /**
   * Default time-out of key encryption keys. Per NIST SP 800-57 P1 R5 section 5.3.6, set to 1 week.
   */
  private static final long KEY_ENCRYPTION_KEY_TIMEOUT = TimeUnit.DAYS.toMillis(7);

  private final String tableKeyId;
  private final int dataKeyLength;

  // a holder class of metadata that is not available after serialization
  private class TransientEncryptionState {
    private final KeyManagementClient kmsClient;
    private final Map<String, WrappedEncryptionKey> encryptionKeys;
    private final LoadingCache<String, ByteBuffer> unwrappedKeyCache;
    private WrappedEncryptionKey currentEncryptionKey;

    private TransientEncryptionState(KeyManagementClient kmsClient) {
      this.kmsClient = kmsClient;
      this.encryptionKeys = Maps.newLinkedHashMap();
      this.unwrappedKeyCache =
          Caffeine.newBuilder()
              .expireAfterWrite(1, TimeUnit.HOURS)
              .build(
                  keyId -> kmsClient.unwrapKey(encryptionKeys.get(keyId).wrappedKey(), tableKeyId));
      this.currentEncryptionKey = null;
    }
  }

  private final transient TransientEncryptionState transientState;

  private transient volatile SecureRandom lazyRNG = null;

  /**
   * @deprecated will be removed in 1.8.0. use {@link #StandardEncryptionManager(String, int, List,
   *     KeyManagementClient)} instead.
   */
  @Deprecated
  public StandardEncryptionManager(
      String tableKeyId, int dataKeyLength, KeyManagementClient kmsClient) {
    this(tableKeyId, dataKeyLength, ImmutableList.of(), kmsClient);
  }

  /**
   * @param tableKeyId table encryption key id
   * @param dataKeyLength length of data encryption key (16/24/32 bytes)
   * @param kmsClient Client of KMS used to wrap/unwrap keys in envelope encryption
   */
  StandardEncryptionManager(
      String tableKeyId,
      int dataKeyLength,
      List<WrappedEncryptionKey> keys,
      KeyManagementClient kmsClient) {
    Preconditions.checkNotNull(tableKeyId, "Invalid encryption key ID: null");
    Preconditions.checkArgument(
        dataKeyLength == 16 || dataKeyLength == 24 || dataKeyLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dataKeyLength);
    Preconditions.checkNotNull(kmsClient, "Invalid KMS client: null");
    this.tableKeyId = tableKeyId;
    this.transientState = new TransientEncryptionState(kmsClient);
    this.dataKeyLength = dataKeyLength;

    for (WrappedEncryptionKey key : keys) {
      transientState.encryptionKeys.put(key.id(), key);

      if (transientState.currentEncryptionKey == null
          || transientState.currentEncryptionKey.timestamp() < key.timestamp()) {
        transientState.currentEncryptionKey = key;
      }
    }
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
   * @deprecated will be removed in 1.8.0; use {@link #currentSnapshotKeyId()} instead.
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
   * @deprecated will be removed in 1.8.0; use {@link #unwrapKey(String)}} instead.
   */
  @Deprecated
  public ByteBuffer unwrapKey(ByteBuffer wrappedSecretKey) {
    throw new UnsupportedOperationException("Use unwrapKey(String) instead");
  }

  public String currentSnapshotKeyId() {
    if (transientState == null) {
      throw new IllegalStateException("Cannot return the current snapshot key after serialization");
    }

    if (transientState.currentEncryptionKey == null
        || transientState.currentEncryptionKey.timestamp()
            < System.currentTimeMillis() - KEY_ENCRYPTION_KEY_TIMEOUT) {
      createNewEncryptionKey();
    }

    return transientState.currentEncryptionKey.id();
  }

  public ByteBuffer unwrapKey(String keyId) {
    if (transientState == null) {
      throw new IllegalStateException("Cannot unwrap key after serialization (missing KMS client)");
    }

    return transientState.unwrappedKeyCache.get(keyId);
  }

  private ByteBuffer newKey() {
    byte[] newKey = new byte[dataKeyLength];
    workerRNG().nextBytes(newKey);
    return ByteBuffer.wrap(newKey);
  }

  private String newKeyId() {
    byte[] idBytes = new byte[6];
    workerRNG().nextBytes(idBytes);
    return Base64.getEncoder().encodeToString(idBytes);
  }

  private void createNewEncryptionKey() {
    if (transientState == null) {
      throw new IllegalStateException("Cannot create encryption keys after serialization");
    }

    ByteBuffer unwrapped = newKey();
    ByteBuffer wrapped = transientState.kmsClient.wrapKey(unwrapped, tableKeyId);
    WrappedEncryptionKey key =
        new WrappedEncryptionKey(newKeyId(), wrapped, System.currentTimeMillis());

    // update internal tracking
    transientState.unwrappedKeyCache.put(key.id(), unwrapped);
    transientState.encryptionKeys.put(key.id(), key);
    transientState.currentEncryptionKey = key;
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
