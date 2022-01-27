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
  private long rewriteFilesSize;
  private int totalFilesCount;
  private final StructLike partition;
  private final List<DeltaManifests> manifestsList;

  RewriteFileGroup(StructLike partition) {
    this.latestSequenceNumber = 0;
    this.latestSnapshotId = 0;
    this.rewriteFilesSize = 0;
    this.totalFilesCount = 0;
    this.partition = partition;
    this.manifestsList = Lists.newArrayList();
  }

  private RewriteFileGroup(long latestSequenceNumber, long latestSnapshotId, long rewriteFilesSize, int totalFilesCount,
                           StructLike partition, List<DeltaManifests> manifestsList) {
    this.latestSequenceNumber = latestSequenceNumber;
    this.latestSnapshotId = latestSnapshotId;
    this.rewriteFilesSize = rewriteFilesSize;
    this.totalFilesCount = totalFilesCount;
    this.partition = partition;
    this.manifestsList = manifestsList;
  }

  long latestSequenceNumber() {
    return latestSequenceNumber;
  }

  long latestSnapshotId() {
    return latestSnapshotId;
  }

  StructLike partition() {
    return partition;
  }

  long rewriteFilesSize() {
    return rewriteFilesSize;
  }

  int totalFilesCount() {
    return totalFilesCount;
  }

  List<DeltaManifests> manifestsList() {
    return manifestsList;
  }

  Iterable<ManifestFile> manifestFiles() {
    return Iterables.concat(Lists.transform(manifestsList, DeltaManifests::manifests));
  }

  boolean isEmpty() {
    return manifestsList.isEmpty();
  }

  void add(long sequenceNumber, long snapshotId, long deltaFilesSize, int deltaFilesCount, DeltaManifests manifests)
      throws IOException {
    if (manifests == null || manifests.manifests().isEmpty()) {
      return;
    }

    // v1 table sequence number is always 0.
    if (sequenceNumber >= latestSequenceNumber) {
      this.latestSequenceNumber = sequenceNumber;
      this.latestSnapshotId = snapshotId;
    }

    this.rewriteFilesSize += deltaFilesSize;
    this.totalFilesCount += deltaFilesCount;
    this.manifestsList.add(manifests);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("latestSequenceNumber", latestSequenceNumber)
        .add("latestSnapshotId", latestSnapshotId)
        .add("partition", partition)
        .add("rewriteFilesSize", rewriteFilesSize)
        .add("totalFilesCount", totalFilesCount)
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
      out.writeLong(rewriteFileGroup.rewriteFilesSize());
      out.writeInt(rewriteFileGroup.totalFilesCount());
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
      long rewriteFilesSize = in.readLong();
      int totalFilesCount = in.readInt();
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

      return new RewriteFileGroup(latestSequenceNumber, latestSnapshotId, rewriteFilesSize, totalFilesCount, partition,
          manifestsList);
    }
  }
}
