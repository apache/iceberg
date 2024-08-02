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
import org.junit.jupiter.api.TestTemplate;

public class TestFormatVersions extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testDefaultFormatVersion() {
    assertThat(table.ops().current().formatVersion()).isBetween(1, 2);
  }

  @TestTemplate
  public void testFormatVersionUpgrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    int baseTableVersion = base.formatVersion();
    int newTableVersion = baseTableVersion + 1;
    ops.commit(base, base.upgradeToFormatVersion(newTableVersion));

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

  @TestTemplate
  public void testFormatVersionUpgradeSkipVersion() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    int baseTableVersion = base.formatVersion();

    // exhaustively test upgrading with skipped version(s) to all valid table versions
    for (int tableVersion = baseTableVersion + 2;
        tableVersion <= TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION;
        tableVersion++) {
      final int newTableVersion = tableVersion;
      assertThatThrownBy(() -> ops.commit(base, base.upgradeToFormatVersion(newTableVersion)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              String.format(
                  "Cannot skip format version(s) to upgrade v%d table to v%d",
                  baseTableVersion, newTableVersion));

      assertThat(ops.current().formatVersion()).isEqualTo(baseTableVersion);
    }
  }
}
