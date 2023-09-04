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
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.PartitionsTable.PartitionMap;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

class PartitionStatsMap {
  private final Map<Integer, PartitionSpec> specsById;
  private PartitionMap updatedPartitionMap = null;
  private final Types.StructType partitionType;

  PartitionStatsMap(Map<Integer, PartitionSpec> specsById) {
    this.specsById = Collections.unmodifiableMap(specsById);
    this.partitionType = Partitioning.partitionType(specsById.values());
  }

  void put(ContentFile<?> file) {
    updatePartitionMap(file);
  }

  void put(ManifestFile manifestFile, FileIO io) {
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, io, specsById)) {
      try (CloseableIterable<ManifestEntry<DataFile>> manifestEntries = reader.entries()) {
        manifestEntries.forEach(
            manifestEntry -> {
              Preconditions.checkArgument(manifestEntry.status() == ManifestEntry.Status.ADDED);
              updatePartitionMap(manifestEntry.file());
            });
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  PartitionMap getOrCreatePartitionMap() {
    if (updatedPartitionMap == null) {
      updatedPartitionMap = new PartitionMap(partitionType);
    }

    return updatedPartitionMap;
  }

  private void updatePartitionMap(ContentFile<?> file) {
    int partitionSpecId = file.specId();
    PartitionSpec partitionSpec = specsById.get(partitionSpecId);
    if (!partitionSpec.isPartitioned()) {
      return;
    }

    PartitionData pd = (PartitionData) file.partition();
    Types.StructType specPartitionType = partitionSpec.partitionType();
    if (!specPartitionType.equals(pd.getPartitionType())) {
      // file was created with a different partition spec than table's - bail out as this is an
      // invalid case
      return;
    }

    PartitionMap partitionMap = getOrCreatePartitionMap();
    StructLike partition =
        PartitionUtil.coercePartition(
            partitionType, specsById.get(file.specId()), ((PartitionData) file.partition()).copy());
    // Snapshot info will be dynamically updated during writing stats to file
    // from SnapshotProducer#updatePartitionStatsMapWithParentEntries()
    partitionMap.get(partition).update(file, null);
  }
}
