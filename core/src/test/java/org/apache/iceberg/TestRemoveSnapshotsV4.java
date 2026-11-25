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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestRemoveSnapshotsV4 extends AdaptiveMetadataTestBase {
  private static final String CLEANUP_UNSUPPORTED_MESSAGE =
      "File cleanup for V4 tables is not supported. "
          + "Use CleanupLevel.NONE to skip file cleanup.";

  @AfterEach
  void clearTables() {
    InMemoryTables.clearTables();
  }

  @ParameterizedTest
  @EnumSource(
      value = ExpireSnapshots.CleanupLevel.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = "NONE")
  void rejectsFileCleanupWithRootManifestSnapshot(ExpireSnapshots.CleanupLevel cleanupLevel) {
    Table table = tableWithRootManifestSnapshot();

    assertThatThrownBy(
            () ->
                table
                    .expireSnapshots()
                    .expireOlderThan(table.currentSnapshot().timestampMillis() + 1)
                    .retainLast(1)
                    .cleanupLevel(cleanupLevel)
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessage(CLEANUP_UNSUPPORTED_MESSAGE);

    table.refresh();
    assertThat(table.snapshots()).as("no snapshot should be expired").hasSize(2);
  }

  @Test
  void allowsExpirationWithoutFileCleanupWithRootManifestSnapshot() {
    Table table = tableWithRootManifestSnapshot();
    Set<String> deletedFiles = Sets.newHashSet();

    table
        .expireSnapshots()
        .expireOlderThan(table.currentSnapshot().timestampMillis() + 1)
        .retainLast(1)
        .cleanupLevel(ExpireSnapshots.CleanupLevel.NONE)
        .deleteWith(deletedFiles::add)
        .commit();

    table.refresh();
    assertThat(table.snapshots()).as("one snapshot should be expired").hasSize(1);
    assertThat(deletedFiles).as("no files should be deleted").isEmpty();
  }

  private Table tableWithRootManifestSnapshot() {
    Table table = create("table", SCHEMA, SPEC, 4);
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    return table;
  }
}
