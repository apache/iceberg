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

package com.netflix.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.netflix.iceberg.Files.localInput;

public class TestSnapshotJson {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  public void testJsonConversion() {
    Snapshot expected = new BaseSnapshot(ops, System.currentTimeMillis(),
        "file:/tmp/manifest1.avro", "file:/tmp/manifest2.avro");
    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(ops, json);

    Assert.assertEquals("Snapshot ID should match",
        expected.snapshotId(), snapshot.snapshotId());
    Assert.assertEquals("Files should match",
        expected.manifests(), snapshot.manifests());
    Assert.assertNull("Operation should be null", snapshot.operation());
    Assert.assertNull("Summary should be null", snapshot.summary());
  }

  @Test
  public void testJsonConversionWithOperation() {
    long parentId = 1;
    long id = 2;
    List<ManifestFile> manifests = ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
        new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));

    Snapshot expected = new BaseSnapshot(ops, id, parentId, System.currentTimeMillis(),
        DataOperations.REPLACE, ImmutableMap.of("files-added", "4", "files-deleted", "100"),
        manifests);

    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(ops, json);

    Assert.assertEquals("Snapshot ID should match",
        expected.snapshotId(), snapshot.snapshotId());
    Assert.assertEquals("Timestamp should match",
        expected.timestampMillis(), snapshot.timestampMillis());
    Assert.assertEquals("Parent ID should match",
        expected.parentId(), snapshot.parentId());
    Assert.assertEquals("Manifest list should match",
        expected.manifestListLocation(), snapshot.manifestListLocation());
    Assert.assertEquals("Files should match",
        expected.manifests(), snapshot.manifests());
    Assert.assertEquals("Operation should match",
        expected.operation(), snapshot.operation());
    Assert.assertEquals("Summary should match",
        expected.summary(), snapshot.summary());
  }

  @Test
  public void testJsonConversionWithManifestList() throws IOException {
    long parentId = 1;
    long id = 2;
    List<ManifestFile> manifests = ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
        new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));

    File manifestList = temp.newFile("manifests");
    Assert.assertTrue(manifestList.delete());
    manifestList.deleteOnExit();

    try (ManifestListWriter writer = new ManifestListWriter(
        Files.localOutput(manifestList), id, parentId)) {
      writer.addAll(manifests);
    }

    Snapshot expected = new BaseSnapshot(
        ops, id, parentId, System.currentTimeMillis(), null, null, localInput(manifestList));
    Snapshot inMemory = new BaseSnapshot(
        ops, id, parentId, expected.timestampMillis(), null, null, manifests);

    Assert.assertEquals("Files should match in memory list",
        inMemory.manifests(), expected.manifests());

    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(ops, json);

    Assert.assertEquals("Snapshot ID should match",
        expected.snapshotId(), snapshot.snapshotId());
    Assert.assertEquals("Timestamp should match",
        expected.timestampMillis(), snapshot.timestampMillis());
    Assert.assertEquals("Parent ID should match",
        expected.parentId(), snapshot.parentId());
    Assert.assertEquals("Manifest list should match",
        expected.manifestListLocation(), snapshot.manifestListLocation());
    Assert.assertEquals("Files should match",
        expected.manifests(), snapshot.manifests());
    Assert.assertNull("Operation should be null", snapshot.operation());
    Assert.assertNull("Summary should be null", snapshot.summary());
  }
}
