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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 8.5 — exercise manifest-listing metadata tables on v4 tables.
 *
 * <p>v4 snapshots address the manifest tree via a root manifest (not a manifest list). {@link
 * AllManifestsTable} and {@link ManifestsTable} previously fell back to the table metadata file
 * location for their static-task input URL when {@code manifestListLocation()} was null. The
 * correct v4 location is {@code rootManifestLocation()}; this test pins that behavior.
 */
public class TestV4MetadataTables {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(5)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(7)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  // ManifestsTable's static task input file must point to the v4 root manifest, not the table
  // metadata file fallback.
  @Test
  public void testManifestsTableUsesRootManifestForV4() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap = table.currentSnapshot();

    assertThat(snap.manifestListLocation())
        .as("v4 snapshots must not produce a manifest list")
        .isNull();
    assertThat(snap.rootManifestLocation())
        .as("v4 snapshots must have a root manifest")
        .isNotNull();

    ManifestsTable manifestsTable = new ManifestsTable(table);
    List<FileScanTask> tasks = Lists.newArrayList(manifestsTable.newScan().planFiles());
    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0).file().location())
        .as("ManifestsTable static-task input file must be the v4 root manifest")
        .isEqualTo(snap.rootManifestLocation());
  }

  // AllManifestsTable's per-snapshot StaticDataTask input URL must point to each snapshot's root
  // manifest for v4 snapshots.
  @Test
  public void testAllManifestsTableUsesRootManifestForV4() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    Set<String> expectedLocations =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(Snapshot::rootManifestLocation)
            .collect(Collectors.toSet());

    AllManifestsTable allManifestsTable = new AllManifestsTable(table);
    Set<String> taskInputs =
        StreamSupport.stream(allManifestsTable.newScan().planFiles().spliterator(), false)
            .map(t -> t.file().location())
            .collect(Collectors.toSet());

    assertThat(taskInputs)
        .as("Each AllManifestsTable task must reference its snapshot's v4 root manifest")
        .isEqualTo(expectedLocations);

    // sanity: both snapshots covered
    assertThat(expectedLocations)
        .contains(snap1.rootManifestLocation(), snap2.rootManifestLocation());
  }
}
