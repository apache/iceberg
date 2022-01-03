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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class RewriteFileGroup {

  private long latestSequenceNumber;
  private long latestSnapshotId;
  private int filesCount;
  private long filesSize;
  private final StructLike partition;
  private final List<DeltaManifests> manifestsList;

  RewriteFileGroup(StructLike partition) {
    this.latestSequenceNumber = 0;
    this.latestSnapshotId = 0;
    this.filesCount = 0;
    this.filesSize = 0;
    this.partition = partition;
    this.manifestsList = Lists.newArrayList();
  }

  private RewriteFileGroup(long latestSequenceNumber, long latestSnapshotId, int filesCount, long filesSize,
                           StructLike partition, List<DeltaManifests> manifestsList) {
    this.latestSequenceNumber = latestSequenceNumber;
    this.latestSnapshotId = latestSnapshotId;
    this.filesCount = filesCount;
    this.filesSize = filesSize;
    this.partition = partition;
    this.manifestsList = manifestsList;
  }

  long latestSequenceNumber() {
    return latestSequenceNumber;
  }

  long latestSnapshotId() {
    return latestSnapshotId;
  }

  int filesCount() {
    return filesCount;
  }

  long filesSize() {
    return filesSize;
  }

  List<DeltaManifests> manifestsList() {
    return manifestsList;
  }

  StructLike partition() {
    return partition;
  }

  Iterable<ManifestFile> manifestFiles() {
    return Iterables.concat(Lists.transform(manifestsList, DeltaManifests::manifests));
  }

  void add(int dataFilesCount, long dataFliesSize, long sequenceNumber, long snapshotId, DeltaManifests deltaManifests)
      throws IOException {
    if (deltaManifests == null || deltaManifests.manifests().isEmpty()) {
      return;
    }

    // v1 table sequence number is always 0.
    if (sequenceNumber >= latestSequenceNumber) {
      latestSequenceNumber = sequenceNumber;
      latestSnapshotId = snapshotId;
    }

    filesCount += dataFilesCount;
    filesSize += dataFliesSize;
    manifestsList.add(deltaManifests);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("latestSequenceNumber", latestSequenceNumber)
        .add("latestSnapshotId", latestSnapshotId)
        .add("partition", partition)
        .add("dataFilesCount", filesCount)
        .add("dataFilesSize", filesSize)
        .toString();
  }

  static class Serializer implements SimpleVersionedSerializer<RewriteFileGroup> {

    static final Serializer INSTANCE = new Serializer();

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(RewriteFileGroup rewriteFileGroup) throws IOException {
      Preconditions.checkNotNull(rewriteFileGroup, "RewriteFileGroup to be serialized should not be null");

      ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(binaryOut);

      out.writeLong(rewriteFileGroup.latestSequenceNumber());
      out.writeLong(rewriteFileGroup.latestSnapshotId());
      out.writeInt(rewriteFileGroup.filesCount());
      out.writeLong(rewriteFileGroup.filesSize());
      out.writeObject(rewriteFileGroup.partition());

      int size = rewriteFileGroup.manifestsList().size();
      out.writeInt(size);
      for (DeltaManifests manifests : rewriteFileGroup.manifestsList()) {
        byte[] data = SimpleVersionedSerialization.writeVersionAndSerialize(
                DeltaManifestsSerializer.INSTANCE, manifests);
        out.writeInt(data.length);
        out.write(data);
      }
      out.flush();
      return binaryOut.toByteArray();
    }

    @Override
    public RewriteFileGroup deserialize(int version, byte[] serialized) throws IOException {
      if (version == 1) {
        return deserializeV1(serialized);
      } else {
        throw new RuntimeException("Unknown serialize version: " + version);
      }
    }

    private RewriteFileGroup deserializeV1(byte[] serialized) throws IOException {
      ByteArrayInputStream binaryIn = new ByteArrayInputStream(serialized);
      ObjectInputStream in = new ObjectInputStream(binaryIn);

      long latestSequenceNumber = in.readLong();
      long latestSnapshotId = in.readLong();
      int filesCount = in.readInt();
      long filesSize = in.readLong();
      StructLike partition;
      try {
        partition = (StructLike) in.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException("Fail to read partition bytes", e);
      }

      int size = in.readInt();
      List<DeltaManifests> manifestsList = Lists.newArrayListWithCapacity(size);
      for (int i = 0; i < size; i++) {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        DeltaManifests deltaManifests = SimpleVersionedSerialization.readVersionAndDeSerialize(
                DeltaManifestsSerializer.INSTANCE, data);
        manifestsList.add(deltaManifests);
      }

      return new RewriteFileGroup(latestSequenceNumber, latestSnapshotId, filesCount, filesSize, partition,
          manifestsList);
    }
  }
}
