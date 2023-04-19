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

import org.junit.Assert;
import org.junit.Test;

public class TestFormatVersions extends TableTestBase {
  public TestFormatVersions() {
    super(1);
  }

  @Test
  public void testDefaultFormatVersion() {
    Assert.assertEquals("Should default to v1", 1, table.ops().current().formatVersion());
  }

  @Test
  public void testFormatVersionUpgrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    Assert.assertEquals("Should report v2", 2, ops.current().formatVersion());
  }

  @Test
  public void testFormatVersionDowngrade() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    Assert.assertEquals("Should report v2", 2, ops.current().formatVersion());

    AssertHelpers.assertThrows(
        "Should reject a version downgrade",
        IllegalArgumentException.class,
        "Cannot downgrade",
        () -> ops.current().upgradeToFormatVersion(1));

    Assert.assertEquals("Should report v2", 2, ops.current().formatVersion());
  }

  @Test
  public void testFormatVersionUpgradeNotSupported() {
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    AssertHelpers.assertThrows(
        "Should reject an unsupported version upgrade",
        IllegalArgumentException.class,
        "Cannot upgrade table to unsupported format version",
        () ->
            ops.commit(
                base,
                base.upgradeToFormatVersion(TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1)));

    Assert.assertEquals("Should report v1", 1, ops.current().formatVersion());
  }
}
