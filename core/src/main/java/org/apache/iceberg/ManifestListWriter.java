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
package org.apache.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

abstract class ManifestListWriter implements FileAppender<ManifestFile> {
  private final long snapshotId;
  private final StandardEncryptionManager em;
  private final NativeEncryptionKeyMetadata keyMetadata;
  private final FileAppender<ManifestFile> writer;
  private final OutputFile file;

  private ManifestListWriter(
      OutputFile file, EncryptionManager em, long snapshotId, Map<String, String> meta) {
    if (em instanceof StandardEncryptionManager) {
      // only encrypt the manifest list if standard table encryption is used because the ability to
      // encrypt the manifest list key was introduced for standard encryption.
      this.em = (StandardEncryptionManager) em;
      NativeEncryptionOutputFile encryptedFile = this.em.encrypt(file);
      this.file = encryptedFile.encryptingOutputFile();
      this.keyMetadata = encryptedFile.keyMetadata();
    } else {
      this.em = null;
      this.file = file;
      this.keyMetadata = null;
    }

    this.snapshotId = snapshotId;
    this.writer = newAppender(this.file, meta);
  }

  protected abstract ManifestFile prepare(ManifestFile manifest);

  protected abstract FileAppender<ManifestFile> newAppender(
      OutputFile outputFile, Map<String, String> meta);

  public ManifestListFile toManifestListFile() {
    if (em != null) {
      String keyId = em.currentSnapshotKeyId();
      ByteBuffer encryptedKeyMetadata =
          EncryptionUtil.encryptSnapshotKeyMetadata(
              em.unwrapKey(keyId), snapshotId, keyMetadata.copyWithLength(writer.length()));
      return new BaseManifestListFile(file.location(), snapshotId, keyId, encryptedKeyMetadata);
    } else {
      return new BaseManifestListFile(file.location(), snapshotId, null, null);
    }
  }

  @Override
  public void add(ManifestFile manifest) {
    writer.add(prepare(manifest));
  }

  @Override
  public void addAll(Iterator<ManifestFile> values) {
    values.forEachRemaining(this::add);
  }

  @Override
  public void addAll(Iterable<ManifestFile> values) {
    values.forEach(this::add);
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public long length() {
    return writer.length();
  }

  static class V3Writer extends ManifestListWriter {
    private final V3Metadata.IndexedManifestFile wrapper;

    V3Writer(
        OutputFile snapshotFile,
        EncryptionManager encryptionManager,
        long snapshotId,
        Long parentSnapshotId,
        long sequenceNumber) {
      super(
          snapshotFile,
          encryptionManager,
          snapshotId,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "sequence-number", String.valueOf(sequenceNumber),
              "format-version", "3"));
      this.wrapper = new V3Metadata.IndexedManifestFile(snapshotId, sequenceNumber);
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return Avro.write(file)
            .schema(V3Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create snapshot list writer for path: %s", file);
      }
    }
  }

  static class V2Writer extends ManifestListWriter {
    private final V2Metadata.IndexedManifestFile wrapper;

    V2Writer(
        OutputFile snapshotFile,
        EncryptionManager encryptionManager,
        long snapshotId,
        Long parentSnapshotId,
        long sequenceNumber) {
      super(
          snapshotFile,
          encryptionManager,
          snapshotId,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "sequence-number", String.valueOf(sequenceNumber),
              "format-version", "2"));
      this.wrapper = new V2Metadata.IndexedManifestFile(snapshotId, sequenceNumber);
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return Avro.write(file)
            .schema(V2Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create snapshot list writer for path: %s", file);
      }
    }
  }

  static class V1Writer extends ManifestListWriter {
    private final V1Metadata.IndexedManifestFile wrapper = new V1Metadata.IndexedManifestFile();

    V1Writer(
        OutputFile snapshotFile,
        EncryptionManager encryptionManager,
        long snapshotId,
        Long parentSnapshotId) {
      super(
          snapshotFile,
          encryptionManager,
          snapshotId,
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "format-version", "1"));
    }

    @Override
    protected ManifestFile prepare(ManifestFile manifest) {
      Preconditions.checkArgument(
          manifest.content() == ManifestContent.DATA,
          "Cannot store delete manifests in a v1 table");
      return wrapper.wrap(manifest);
    }

    @Override
    protected FileAppender<ManifestFile> newAppender(OutputFile file, Map<String, String> meta) {
      try {
        return Avro.write(file)
            .schema(V1Metadata.MANIFEST_LIST_SCHEMA)
            .named("manifest_file")
            .meta(meta)
            .overwrite()
            .build();

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create snapshot list writer for path: %s", file);
      }
    }
  }
}
