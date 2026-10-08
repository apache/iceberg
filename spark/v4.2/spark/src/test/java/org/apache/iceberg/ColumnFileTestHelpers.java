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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public final class ColumnFileTestHelpers {
  private static final int FORMAT_VERSION = 4;

  private ColumnFileTestHelpers() {}

  /**
   * Returns a copy of a data file that references the given column files.
   *
   * <p>Only the fields a reader needs to locate and read the file are carried over.
   */
  public static DataFile withColumnFiles(
      DataFile file, PartitionSpec spec, List<ColumnFile> columnFiles) {
    Preconditions.checkArgument(
        file.specId() == spec.specId(),
        "Invalid data file: spec ID %s does not match %s",
        file.specId(),
        spec.specId());

    TrackedFile tracked =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            FORMAT_VERSION,
            file.location(),
            file.format(),
            file.recordCount(),
            file.fileSizeInBytes(),
            spec.isUnpartitioned() ? null : spec.specId(),
            spec.isUnpartitioned() ? null : partitionData(spec, file.partition()),
            null,
            null,
            null,
            null,
            file.keyMetadata(),
            file.splitOffsets(),
            null,
            columnFiles);

    return TrackedFileAdapters.asDataFile(tracked, ImmutableMap.of(spec.specId(), spec));
  }

  private static PartitionData partitionData(PartitionSpec spec, StructLike partition) {
    PartitionData data = new PartitionData(spec.partitionType());
    for (int pos = 0; pos < data.size(); pos += 1) {
      data.set(pos, partition.get(pos, Object.class));
    }

    return data;
  }
}
