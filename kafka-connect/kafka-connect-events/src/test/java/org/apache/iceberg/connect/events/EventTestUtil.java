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
package org.apache.iceberg.connect.events;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

public class EventTestUtil {
  private EventTestUtil() {}

  public static DataFile createDataFile() {
    Schema schema =
        new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.LongType.get())));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").withSpecId(1).build();
    SortOrder order =
        SortOrder.builderFor(schema).sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST).build();

    Metrics metrics =
        new Metrics(
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());

    return DataFiles.builder(spec)
        .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[] {0}))
        .withFileSizeInBytes(1L)
        .withFormat(FileFormat.PARQUET)
        .withMetrics(metrics)
        .withPath("path")
        .withSortOrder(order)
        .withSplitOffsets(ImmutableList.of(4L))
        .build();
  }

  public static DeleteFile createDeleteFile() {
    Schema schema =
        new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.LongType.get())));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").withSpecId(1).build();
    SortOrder order =
        SortOrder.builderFor(schema).sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST).build();

    Metrics metrics =
        new Metrics(
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());

    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(1)
        .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[] {0}))
        .withFileSizeInBytes(1L)
        .withFormat(FileFormat.PARQUET)
        .withMetrics(metrics)
        .withPath("path")
        .withSortOrder(order)
        .withSplitOffsets(ImmutableList.of(4L))
        .build();
  }
}
