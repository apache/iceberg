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
package org.apache.iceberg.flink.sink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
public class DeltaManifestsSerializer implements SimpleVersionedSerializer<DeltaManifests> {
  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;
  private static final int VERSION_3 = 3;
  private static final byte[] EMPTY_BINARY = new byte[0];

  public static final DeltaManifestsSerializer INSTANCE = new DeltaManifestsSerializer(VERSION_2);

  static final DeltaManifestsSerializer DV_ONLY = new DeltaManifestsSerializer(VERSION_3);

  private final int writeVersion;

  private DeltaManifestsSerializer(int writeVersion) {
    this.writeVersion = writeVersion;
  }

  @Override
  public int getVersion() {
    return writeVersion;
  }

  @Override
  public byte[] serialize(DeltaManifests deltaManifests) throws IOException {
    Preconditions.checkNotNull(
        deltaManifests, "DeltaManifests to be serialized should not be null");
    return writeVersion == VERSION_2 ? serializeV2(deltaManifests) : serializeV3(deltaManifests);
  }

  private static byte[] serializeV2(DeltaManifests deltaManifests) throws IOException {
    Preconditions.checkArgument(
        deltaManifests.deleteManifests().size() <= 1
            && deltaManifests.rewrittenDeleteManifests().isEmpty()
            && deltaManifests.baselineSnapshotId() == null,
        "Cannot serialize the manifests of the DV-only write path as version %s",
        VERSION_2);

    ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(binaryOut);

    writeManifest(out, deltaManifests.dataManifest());
    List<ManifestFile> deleteManifests = deltaManifests.deleteManifests();
    writeManifest(out, deleteManifests.isEmpty() ? null : deleteManifests.get(0));
    writeReferencedDataFiles(out, deltaManifests.referencedDataFiles());

    return binaryOut.toByteArray();
  }

  private static byte[] serializeV3(DeltaManifests deltaManifests) throws IOException {
    ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(binaryOut);

    writeManifest(out, deltaManifests.dataManifest());
    writeManifests(out, deltaManifests.deleteManifests());
    writeReferencedDataFiles(out, deltaManifests.referencedDataFiles());
    writeManifests(out, deltaManifests.rewrittenDeleteManifests());

    Long baselineSnapshotId = deltaManifests.baselineSnapshotId();
    out.writeBoolean(baselineSnapshotId != null);
    if (baselineSnapshotId != null) {
      out.writeLong(baselineSnapshotId);
    }

    return binaryOut.toByteArray();
  }

  @Override
  public DeltaManifests deserialize(int version, byte[] serialized) throws IOException {
    return switch (version) {
      case VERSION_1 -> deserializeV1(serialized);
      case VERSION_2 -> deserializeV2(serialized);
      case VERSION_3 -> deserializeV3(serialized);
      default -> throw new RuntimeException("Unknown serialize version: " + version);
    };
  }

  private DeltaManifests deserializeV1(byte[] serialized) throws IOException {
    return new DeltaManifests(ManifestFiles.decode(serialized), null);
  }

  private DeltaManifests deserializeV2(byte[] serialized) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));

    ManifestFile dataManifest = readManifest(in);
    ManifestFile deleteManifest = readManifest(in);
    CharSequence[] referencedDataFiles = readReferencedDataFiles(in);

    return new DeltaManifests(
        dataManifest, listOf(deleteManifest), ImmutableList.of(), referencedDataFiles, null);
  }

  private DeltaManifests deserializeV3(byte[] serialized) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));

    ManifestFile dataManifest = readManifest(in);
    List<ManifestFile> deleteManifests = readManifests(in);
    CharSequence[] referencedDataFiles = readReferencedDataFiles(in);
    List<ManifestFile> rewrittenDeleteManifests = readManifests(in);
    Long baselineSnapshotId = in.readBoolean() ? in.readLong() : null;

    return new DeltaManifests(
        dataManifest,
        deleteManifests,
        rewrittenDeleteManifests,
        referencedDataFiles,
        baselineSnapshotId);
  }

  private static void writeReferencedDataFiles(
      DataOutputStream out, CharSequence[] referencedDataFiles) throws IOException {
    out.writeInt(referencedDataFiles.length);
    for (CharSequence referencedDataFile : referencedDataFiles) {
      out.writeUTF(referencedDataFile.toString());
    }
  }

  private static CharSequence[] readReferencedDataFiles(DataInputStream in) throws IOException {
    int referenceDataFileNum = in.readInt();
    CharSequence[] referencedDataFiles = new CharSequence[referenceDataFileNum];
    for (int i = 0; i < referenceDataFileNum; i++) {
      referencedDataFiles[i] = in.readUTF();
    }

    return referencedDataFiles;
  }

  private static List<ManifestFile> listOf(ManifestFile manifest) {
    return manifest != null ? ImmutableList.of(manifest) : ImmutableList.of();
  }

  private static void writeManifests(DataOutputStream out, List<ManifestFile> manifests)
      throws IOException {
    out.writeInt(manifests.size());
    for (ManifestFile manifest : manifests) {
      writeManifest(out, manifest);
    }
  }

  private static List<ManifestFile> readManifests(DataInputStream in) throws IOException {
    int count = in.readInt();
    List<ManifestFile> manifests = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      manifests.add(readManifest(in));
    }

    return manifests;
  }

  private static void writeManifest(DataOutputStream out, ManifestFile manifest)
      throws IOException {
    byte[] binary = manifest != null ? ManifestFiles.encode(manifest) : EMPTY_BINARY;
    out.writeInt(binary.length);
    out.write(binary);
  }

  private static ManifestFile readManifest(DataInputStream in) throws IOException {
    int size = in.readInt();
    if (size <= 0) {
      return null;
    }

    byte[] binary = new byte[size];
    in.readFully(binary);
    return ManifestFiles.decode(binary);
  }
}
