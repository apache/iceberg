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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataFileGroup implements KryoSerializable {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileGroup.class);

  private long latestSequenceNumber;
  private long lastSnapshotId;
  private int filesCount;
  private long filesSize;
  private transient List<DeltaManifests> manifestsList;

  DataFileGroup() {
    this.latestSequenceNumber = 0;
    this.lastSnapshotId = 0;
    this.filesCount = 0;
    this.filesSize = 0;
    this.manifestsList = Lists.newArrayList();
  }

  long latestSequenceNumber() {
    return latestSequenceNumber;
  }

  long latestSnapshotId() {
    return lastSnapshotId;
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

  Iterable<ManifestFile> manifestFiles() {
    return Iterables.concat(Lists.transform(manifestsList, DeltaManifests::manifests));
  }

  void append(int dataFilesCount, long dataFliesSize, long snapshotId, long sequenceNumber,
              DeltaManifests deltaManifests) throws IOException {
    if (deltaManifests == null || deltaManifests.manifests().isEmpty()) {
      return;
    }

    if (sequenceNumber > latestSequenceNumber) {
      latestSequenceNumber = sequenceNumber;
      lastSnapshotId = snapshotId;
    }

    filesCount += dataFilesCount;
    filesSize += dataFliesSize;
    manifestsList.add(deltaManifests);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    try {
      output.writeLong(latestSequenceNumber);
      output.writeLong(lastSnapshotId);
      output.writeInt(filesCount);
      output.writeLong(filesSize);

      int size = manifestsList.size();
      output.writeInt(size);
      for (DeltaManifests manifests : manifestsList) {
        byte[] data = SimpleVersionedSerialization.writeVersionAndSerialize(
            DeltaManifestsSerializer.INSTANCE, manifests);
        output.writeInt(data.length);
        output.writeBytes(data);
      }
    } catch (IOException e) {
      LOG.error("Cannot serialize data file group by kryo.", e);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {

    try {
      latestSequenceNumber = input.readLong();
      lastSnapshotId = input.readLong();
      filesCount = input.readInt();
      filesSize = input.readLong();

      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        int length = input.readInt();
        byte[] data = input.readBytes(length);
        DeltaManifests deltaManifests = SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, data);
        manifestsList.add(deltaManifests);
      }
    } catch (IOException e) {
      LOG.error("Cannot deserialize data file group by kryo.", e);

      filesCount = 0;
      filesSize = 0;
      manifestsList = null;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("latestSequenceNumber", latestSequenceNumber)
        .add("lastSnapshotId", lastSnapshotId)
        .add("dataFilesCount", filesCount)
        .add("dataFilesSize", filesSize)
        .add("deltaManifestsList", manifestsList)
        .toString();
  }
}
