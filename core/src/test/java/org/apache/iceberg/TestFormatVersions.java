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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.TestTemplate;

public class TestFormatVersions extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testFormatVersionUpgrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    int baseTableVersion = base.formatVersion();
    int newTableVersion = baseTableVersion + 1;

    TableMetadata newTableMetadata = base.upgradeToFormatVersion(newTableVersion);

    assertThat(
            newTableMetadata.changes().stream()
                .filter(
                    metadataUpdate -> metadataUpdate instanceof MetadataUpdate.UpgradeFormatVersion)
                .map(
                    metadataUpdate ->
                        ((MetadataUpdate.UpgradeFormatVersion) metadataUpdate).formatVersion()))
        .isEqualTo(List.of(newTableVersion));

    ops.commit(base, newTableMetadata);

    assertThat(ops.current().formatVersion()).isEqualTo(newTableVersion);
  }

  @TestTemplate
  public void testFormatVersionUpgradeToLatest() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    int baseTableVersion = base.formatVersion();
    int newTableVersion = TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION;

    TableMetadata newTableMetadata = base.upgradeToFormatVersion(newTableVersion);

    // check that non-incremental updates are syntactic sugar for serial updates. E.g. upgrading
    // from V1 to V3 will
    // register changes in the table metadata for upgrading to V2 and V3 in order (V1->V2->V3)
    assertThat(
            newTableMetadata.changes().stream()
                .filter(
                    metadataUpdate -> metadataUpdate instanceof MetadataUpdate.UpgradeFormatVersion)
                .map(
                    metadataUpdate ->
                        ((MetadataUpdate.UpgradeFormatVersion) metadataUpdate).formatVersion()))
        .isEqualTo(
            IntStream.rangeClosed(baseTableVersion + 1, newTableVersion)
                .boxed()
                .collect(Collectors.toList()));

    ops.commit(base, newTableMetadata);

    assertThat(ops.current().formatVersion()).isEqualTo(newTableVersion);
  }

  @TestTemplate
  public void testFormatVersionDowngrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    int baseTableVersion = base.formatVersion();
    int newTableVersion = baseTableVersion + 1;
    ops.commit(base, base.upgradeToFormatVersion(newTableVersion));

    assertThat(ops.current().formatVersion()).isEqualTo(newTableVersion);

    assertThatThrownBy(() -> ops.current().upgradeToFormatVersion(baseTableVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format("Cannot downgrade v%d table to v%d", newTableVersion, baseTableVersion));

    assertThat(ops.current().formatVersion()).isEqualTo(newTableVersion);
  }

  @TestTemplate
  public void testFormatVersionUpgradeNotSupported() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    int baseTableVersion = base.formatVersion();
    int unsupportedTableVersion = TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1;

    assertThatThrownBy(() -> ops.commit(base, base.upgradeToFormatVersion(unsupportedTableVersion)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Cannot upgrade table to unsupported format version: v%d (supported: v%d)",
                unsupportedTableVersion, TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION));

    assertThat(ops.current().formatVersion()).isEqualTo(baseTableVersion);
  }
}
