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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for concurrent manifest filtering in ManifestFilterManager.
 *
 * <p>These tests verify that parallel manifest filtering does not corrupt shared mutable state
 * (specifically the deleteFiles set) when multiple worker threads process manifests concurrently.
 *
 * @see <a href="https://github.com/apache/iceberg/issues/16978">Issue #16978</a>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestFilterManagerConcurrency extends TestBase {

  @Parameter(index = 0)
  private int formatVersion;

  @Parameter(index = 1)
  private String branch;

  @Parameters(name = "formatVersion = {0}, branch = {1}")
  protected static List<Object> parameters() {
    return TestHelpers.ALL_VERSIONS.stream()
        .flatMap(
            v ->
                Stream.of(
                    new Object[] {v, SnapshotRef.MAIN_BRANCH}, new Object[] {v, "testBranch"}))
        .collect(Collectors.toList());
  }

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();

  private static final int MANIFEST_COUNT = 32;
  private static final int FILES_PER_MANIFEST = 4;

  @TestTemplate
  public void testParallelOverwriteDoesNotCorruptDeleteFiles() {
    // Create a table with many manifests to ensure parallel processing
    table = create(SCHEMA, SPEC);

    // Append files across multiple manifests by committing in batches
    for (int m = 0; m < MANIFEST_COUNT; m++) {
      int manifestIndex = m;
      List<DataFile> files =
          IntStream.range(0, FILES_PER_MANIFEST)
              .mapToObj(
                  f ->
                      DataFiles.builder(SPEC)
                          .withPath("/path/to/data-" + manifestIndex + "-" + f + ".parquet")
                          .withFileSizeInBytes(10)
                          .withPartitionPath("data=batch" + manifestIndex)
                          .withRecordCount(1)
                          .build())
              .collect(Collectors.toList());

      AppendFiles append = table.newAppend();
      files.forEach(append::appendFile);
      commit(table, append, branch);
    }

    // Verify we have the expected number of manifests
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(MANIFEST_COUNT);

    // Perform a full overwrite using a row filter that matches all rows.
    // This triggers parallel manifest filtering and previously caused
    // concurrent mutation of the shared deleteFiles set.
    OverwriteFiles overwrite = table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue());

    // The overwrite should complete without throwing ClassCastException
    // or any other concurrent modification error
    assertThatNoException().isThrownBy(() -> commit(table, overwrite, branch));

    // Verify the overwrite succeeded and all old files were deleted
    assertThat(table.currentSnapshot().addedDataFiles(table.io())).isEmpty();
    assertThat(table.currentSnapshot().removedDataFiles(table.io())).isEmpty();
  }

  @TestTemplate
  public void testParallelDeleteByExpressionCollectsAllFiles() {
    table = create(SCHEMA, SPEC);

    // Append files across multiple manifests
    for (int m = 0; m < MANIFEST_COUNT; m++) {
      int manifestIndex = m;
      List<DataFile> files =
          IntStream.range(0, FILES_PER_MANIFEST)
              .mapToObj(
                  f ->
                      DataFiles.builder(SPEC)
                          .withPath("/path/to/data-" + manifestIndex + "-" + f + ".parquet")
                          .withFileSizeInBytes(10)
                          .withPartitionPath("data=batch" + manifestIndex)
                          .withRecordCount(1)
                          .build())
              .collect(Collectors.toList());

      AppendFiles append = table.newAppend();
      files.forEach(append::appendFile);
      commit(table, append, branch);
    }

    // Delete files by partition expression that matches all partitions
    DeleteFiles delete = table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue());

    assertThatNoException().isThrownBy(() -> commit(table, delete, branch));

    // After deleting all files, the table should have no data files
    assertThat(table.currentSnapshot().dataManifests(table.io())).isEmpty();
  }
}
