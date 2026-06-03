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
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestUuidStrictMetricsIntegration extends TestBase {
  private static final String DELETE_TABLE_NAME = "uuid_delete_table";
  private static final String OVERWRITE_TABLE_NAME = "uuid_overwrite_table";
  private static final String DELETE_FILE_PATH = "/path/to/data/legacy-uuid-delete.parquet";
  private static final String OVERWRITE_FILE_PATH = "/path/to/data/legacy-uuid-overwrite.parquet";

  private static final int UUID_FIELD_ID = 2;

  private static final UUID UUID_40 = UUID.fromString("40000000-0000-0000-0000-000000000001");
  private static final UUID UUID_80 = UUID.fromString("80000000-0000-0000-0000-000000000001");

  private static final Schema UUID_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(UUID_FIELD_ID, "uuid_col", Types.UUIDType.get()));

  private static final PartitionSpec UUID_SPEC = PartitionSpec.unpartitioned();

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

  @TestTemplate
  public void deleteByRowFilterDoesNotUseLegacySignedUuidMetricsToDeleteWholeFiles() {
    Table uuidTable =
        TestTables.create(tableDir, DELETE_TABLE_NAME, UUID_SCHEMA, UUID_SPEC, formatVersion);
    DataFile legacyFile = legacySignedUuidFile(DELETE_FILE_PATH);

    Snapshot append = commit(uuidTable, uuidTable.newFastAppend().appendFile(legacyFile), branch);

    assertThatThrownBy(
            () ->
                commit(
                    uuidTable,
                    uuidTable
                        .newDelete()
                        .deleteFromRowFilter(Expressions.lessThanOrEqual("uuid_col", UUID_40)),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot delete file where some, but not all, rows match filter");

    assertThat(latestSnapshot(uuidTable, branch).snapshotId()).isEqualTo(append.snapshotId());
  }

  @TestTemplate
  public void overwriteValidationDoesNotUseLegacySignedUuidMetricsToAcceptAddedFiles() {
    Table uuidTable =
        TestTables.create(tableDir, OVERWRITE_TABLE_NAME, UUID_SCHEMA, UUID_SPEC, formatVersion);
    DataFile legacyFile = legacySignedUuidFile(OVERWRITE_FILE_PATH);

    OverwriteFiles overwrite =
        uuidTable
            .newOverwrite()
            .overwriteByRowFilter(Expressions.lessThanOrEqual("uuid_col", UUID_40))
            .addFile(legacyFile)
            .validateAddedFilesMatchOverwriteFilter();

    assertThatThrownBy(() -> commit(uuidTable, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot append file with rows that do not match filter");
  }

  private static DataFile legacySignedUuidFile(String path) {
    return DataFiles.builder(UUID_SPEC)
        .withPath(path)
        .withFileSizeInBytes(10)
        .withMetrics(
            new Metrics(
                4L,
                null,
                ImmutableMap.of(UUID_FIELD_ID, 4L),
                ImmutableMap.of(UUID_FIELD_ID, 0L),
                null,
                ImmutableMap.of(UUID_FIELD_ID, uuidToBuffer(UUID_80)),
                ImmutableMap.of(UUID_FIELD_ID, uuidToBuffer(UUID_40))))
        .build();
  }

  private static ByteBuffer uuidToBuffer(UUID uuid) {
    return Conversions.toByteBuffer(Types.UUIDType.get(), uuid);
  }
}
