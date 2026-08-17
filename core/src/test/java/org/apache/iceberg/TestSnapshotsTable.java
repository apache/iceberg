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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

public class TestSnapshotsTable {

  private static final String LOCATION = "file:/tmp/snap-1.avro";

  @Test
  public void legacySnapshotPopulatesManifestListAndSnapshotFile() {
    Snapshot snap = stubSnapshot(ManifestFile.LEGACY_FORMAT_VERSION);
    StaticDataTask.Row row = SnapshotsTable.snapshotToRow(snap);

    assertThat(row.get(4, String.class))
        .as("manifest_list should be populated for legacy snapshots")
        .isEqualTo(LOCATION);
    assertThat(row.get(6, String.class))
        .as("snapshot_file should always be populated")
        .isEqualTo(LOCATION);
  }

  @Test
  public void adaptiveSnapshotNullsManifestListAndPopulatesSnapshotFile() {
    Snapshot snap = stubSnapshot(4);
    StaticDataTask.Row row = SnapshotsTable.snapshotToRow(snap);

    assertThat(row.get(4, String.class))
        .as("manifest_list should be null for adaptive snapshots")
        .isNull();
    assertThat(row.get(6, String.class))
        .as("snapshot_file should be populated")
        .isEqualTo(LOCATION);
  }

  private static Snapshot stubSnapshot(int formatVersion) {
    Snapshot snap = mock(Snapshot.class);
    when(snap.formatVersion()).thenReturn(formatVersion);
    when(snap.snapshotFileLocation()).thenReturn(LOCATION);
    when(snap.timestampMillis()).thenReturn(1L);
    when(snap.snapshotId()).thenReturn(1L);
    when(snap.parentId()).thenReturn(null);
    when(snap.operation()).thenReturn("append");
    when(snap.summary()).thenReturn(null);
    return snap;
  }
}
