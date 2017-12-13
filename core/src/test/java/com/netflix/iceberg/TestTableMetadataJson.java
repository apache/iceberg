/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import java.util.Random;

public class TestTableMetadataJson {
  @Test
  public void testJsonConversion() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.LongType.get()),
        Types.NestedField.required(2, "y", Types.LongType.get()),
        Types.NestedField.required(3, "z", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).build();

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(null, previousSnapshotId, "file:/tmp/manfiest.1.avro");
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(null, currentSnapshotId, "file:/tmp/manfiest.2.avro");

    TableMetadata expected = new TableMetadata(null, null,
        System.currentTimeMillis(), 3, schema, spec, ImmutableMap.of("property", "value"),
        currentSnapshotId, Arrays.asList(previousSnapshot, currentSnapshot));

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(null, null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Last column ID should match",
        expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals("Schema should match",
        expected.schema().asStruct(), metadata.schema().asStruct());
    Assert.assertEquals("Partition spec should match",
        expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals("Properties should match",
        expected.properties(), metadata.properties());
    Assert.assertEquals("Current snapshot ID should match",
        currentSnapshotId, metadata.currentSnapshot().snapshotId());
    Assert.assertEquals("Current snapshot files should match",
        currentSnapshot.manifests(), metadata.currentSnapshot().manifests());
    Assert.assertEquals("Previous snapshot ID should match",
        previousSnapshotId, metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals("Previous snapshot files should match",
        previousSnapshot.manifests(),
        metadata.snapshot(previousSnapshotId).manifests());
  }
}
