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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class EncryptingFileIO implements FileIO, Serializable {
  public static EncryptingFileIO create(FileIO io, EncryptionManager em) {
    return new EncryptingFileIO(io, em);
  }

  private final FileIO io;
  private final EncryptionManager em;

  EncryptingFileIO(FileIO io, EncryptionManager em) {
    this.io = io;
    this.em = em;
  }

  public Map<String, InputFile> bulkDecrypt(Iterable<? extends ContentFile<?>> files) {
    Iterable<InputFile> decrypted = em.decrypt(Iterables.transform(files, this::wrap));

    ImmutableMap.Builder<String, InputFile> builder = ImmutableMap.builder();
    for (InputFile in : decrypted) {
      builder.put(in.location(), in);
    }

    return builder.buildKeepingLast();
  }

  public EncryptionManager encryptionManager() {
    return em;
  }

  @Override
  public InputFile newInputFile(String path) {
    return io.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return io.newInputFile(path, length);
  }

  @Override
  public InputFile newInputFile(DataFile file) {
    return newInputFile((ContentFile<?>) file);
  }

  @Override
  public InputFile newInputFile(DeleteFile file) {
    return newInputFile((ContentFile<?>) file);
  }

  private InputFile newInputFile(ContentFile<?> file) {
    if (file.keyMetadata() != null) {
      return newDecryptingInputFile(
          file.path().toString(), file.fileSizeInBytes(), file.keyMetadata());
    } else {
      return newInputFile(file.path().toString(), file.fileSizeInBytes());
    }
  }

  @Override
  public InputFile newInputFile(ManifestFile manifest) {
    if (manifest.keyMetadata() != null) {
      return newDecryptingInputFile(manifest.path(), manifest.length(), manifest.keyMetadata());
    } else {
      return newInputFile(manifest.path(), manifest.length());
    }
  }

  public InputFile newDecryptingInputFile(String path, ByteBuffer buffer) {
    return em.decrypt(wrap(io.newInputFile(path), buffer));
  }

  public InputFile newDecryptingInputFile(String path, long length, ByteBuffer buffer) {
    // TODO: is the length correct for the encrypted file? It may be the length of the plaintext
    // stream
    return em.decrypt(wrap(io.newInputFile(path, length), buffer));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return io.newOutputFile(path);
  }

  public EncryptedOutputFile newEncryptingOutputFile(String path) {
    OutputFile plainOutputFile = io.newOutputFile(path);
    return em.encrypt(plainOutputFile);
  }

  @Override
  public void deleteFile(String path) {
    io.deleteFile(path);
  }

  @Override
  public void close() {
    io.close();

    if (em instanceof Closeable) {
      try {
        ((Closeable) em).close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close encryption manager", e);
      }
    }
  }

  private SimpleEncryptedInputFile wrap(ContentFile<?> file) {
    InputFile encryptedInputFile = io.newInputFile(file.path().toString(), file.fileSizeInBytes());
    return new SimpleEncryptedInputFile(encryptedInputFile, toKeyMetadata(file.keyMetadata()));
  }

  private static SimpleEncryptedInputFile wrap(InputFile encryptedInputFile, ByteBuffer buffer) {
    return new SimpleEncryptedInputFile(encryptedInputFile, toKeyMetadata(buffer));
  }

  private static EncryptionKeyMetadata toKeyMetadata(ByteBuffer buffer) {
    return buffer != null ? new SimpleKeyMetadata(buffer) : EmptyKeyMetadata.get();
  }

  private static class SimpleEncryptedInputFile implements EncryptedInputFile {
    private final InputFile encryptedInputFile;
    private final EncryptionKeyMetadata keyMetadata;

    private SimpleEncryptedInputFile(
        InputFile encryptedInputFile, EncryptionKeyMetadata keyMetadata) {
      this.encryptedInputFile = encryptedInputFile;
      this.keyMetadata = keyMetadata;
    }

    @Override
    public InputFile encryptedInputFile() {
      return encryptedInputFile;
    }

    @Override
    public EncryptionKeyMetadata keyMetadata() {
      return keyMetadata;
    }
  }

  private static class SimpleKeyMetadata implements EncryptionKeyMetadata {
    private final ByteBuffer metadataBuffer;

    private SimpleKeyMetadata(ByteBuffer metadataBuffer) {
      this.metadataBuffer = metadataBuffer;
    }

    @Override
    public ByteBuffer buffer() {
      return metadataBuffer;
    }

    @Override
    public EncryptionKeyMetadata copy() {
      return new SimpleKeyMetadata(metadataBuffer.duplicate());
    }
  }

  private static class EmptyKeyMetadata implements EncryptionKeyMetadata {
    private static final EmptyKeyMetadata INSTANCE = new EmptyKeyMetadata();

    private static EmptyKeyMetadata get() {
      return INSTANCE;
    }

    @Override
    public ByteBuffer buffer() {
      return null;
    }

    @Override
    public EncryptionKeyMetadata copy() {
      return this;
    }
  }
}
