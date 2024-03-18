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
    return Arrays.asList(1);
  }

  @TestTemplate
  public void testDefaultFormatVersion() {
    assertThat(table.ops().current().formatVersion()).isEqualTo(1);
  }

  @TestTemplate
  public void testFormatVersionUpgrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    assertThat(ops.current().formatVersion()).isEqualTo(2);
  }

  @TestTemplate
  public void testFormatVersionDowngrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    assertThat(ops.current().formatVersion()).isEqualTo(2);

    assertThatThrownBy(() -> ops.current().upgradeToFormatVersion(1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v2 table to v1");

    assertThat(ops.current().formatVersion()).isEqualTo(2);
  }

  @TestTemplate
  public void testFormatVersionUpgradeNotSupported() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();

    assertThatThrownBy(
            () ->
                ops.commit(
                    base,
                    base.upgradeToFormatVersion(TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot upgrade table to unsupported format version: v3 (supported: v2)");

    assertThat(ops.current().formatVersion()).isEqualTo(1);
  }
}
