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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public abstract class TestStreamingOffset extends TestStructuredStreamingRead {

  @Test
  public void testStreamingOffsetWithPosition() throws IOException {
    Snapshot currSnap = table.currentSnapshot();
    StreamingOffset startOffset =
        new StreamingOffset(currSnap.snapshotId(), INIT_SCANNED_FILE_INDEX, false, true);
    StreamingOffset endOffset = startOffset;
    ManifestFile manifest = currSnap.dataManifests().get(0);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      long expectedPos = startOffset.index();
      for (DataFile file : reader) {
        expectedPos += 1;
        Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
        endOffset =
            new StreamingOffset(currSnap.snapshotId(), Math.toIntExact(file.pos()), false, true);
      }
      StreamingOffset expectedOffset =
          new StreamingOffset(currSnap.snapshotId(), (int) expectedPos, false, true);
      Assert.assertEquals(expectedOffset, endOffset);
    }
  }

  @Test
  public void testScanAllFiles() throws IOException {
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests();
    List<StreamingOffset> expectedOffsets = Lists.newArrayList();
    for (ManifestFile manifest : manifests) {
      expectedOffsets
          .add(new StreamingOffset(manifest.snapshotId(), END_SCANNED_FILE_INDEX, true, true));
    }
    testStreamingOffsetWithScanFiles(expectedOffsets, true);
  }

  @Test
  public void testNoScanAllFiles() throws IOException {
    List<StreamingOffset> expectedOffsets = Lists
        .newArrayList(
            new StreamingOffset(table.currentSnapshot().snapshotId(), END_SCANNED_FILE_INDEX, false,
                true));
    testStreamingOffsetWithScanFiles(expectedOffsets, false);
  }

  private void testStreamingOffsetWithScanFiles(List<StreamingOffset> expectedOffsets,
      boolean scanAllFiles) throws IOException {
    Snapshot currSnap = table.currentSnapshot();
    List<ManifestFile> manifests = scanAllFiles ? currSnap.dataManifests() :
        currSnap.dataManifests().stream().filter(m -> m.snapshotId().equals(currSnap.snapshotId()))
            .collect(Collectors.toList());
    List<StreamingOffset> actualOffsets = Lists.newArrayList();
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
        StreamingOffset offset = StreamingOffset.START_OFFSET;
        long expectedPos = INIT_SCANNED_FILE_INDEX;
        for (DataFile file : reader) {
          expectedPos += 1;
          Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
          if (scanAllFiles) {
            offset = new StreamingOffset(manifest.snapshotId(), Math.toIntExact(file.pos()),
                scanAllFiles, true);
          } else {
            offset = new StreamingOffset(currSnap.snapshotId(), Math.toIntExact(file.pos()),
                scanAllFiles, true);
          }
        }
        actualOffsets.add(offset);
      }
    }

    Assert.assertArrayEquals(expectedOffsets.toArray(), actualOffsets.toArray());
  }
}
