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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestPartitionSpecInfo {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private final Schema schema = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get()));
  private File tableDir = null;

  private int newFieldId;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1, 1000 },
        new Object[] { 2, 1001 },
    };
  }

  private final int formatVersion;

  public TestPartitionSpecInfo(int formatVersion, int newFieldId) {
    this.formatVersion = formatVersion;
    this.newFieldId = newFieldId;
  }

  @Before
  public void setupTableDir() throws IOException {
    this.tableDir = temp.newFolder();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testSpecInfoUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    Assert.assertEquals(spec, table.spec());
    Assert.assertEquals(spec.lastAssignedFieldId(), table.spec().lastAssignedFieldId());
    Assert.assertEquals(ImmutableMap.of(spec.specId(), spec), table.specs());
    Assert.assertNull(table.specs().get(Integer.MAX_VALUE));
  }

  @Test
  public void testSpecInfoPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    Assert.assertEquals(spec, table.spec());
    Assert.assertEquals(spec.lastAssignedFieldId(), table.spec().lastAssignedFieldId());
    Assert.assertEquals(ImmutableMap.of(spec.specId(), spec), table.specs());
    Assert.assertNull(table.specs().get(Integer.MAX_VALUE));
  }

  @Test
  public void testSpecInfoPartitionSpecEvolution() {
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .bucket("data", 4)
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    Assert.assertEquals(spec, table.spec());

    PartitionSpec expectedNewSpec = PartitionSpec.builderFor(table.schema())
        .add(2, newFieldId, "data_bucket", "bucket[10]")
        .withSpecId(1)
        .build();

    table.updateSpec().clear()
        .bucket("data", 10)
        .commit();

    Assert.assertEquals(expectedNewSpec, table.spec());
    Assert.assertEquals(expectedNewSpec, table.specs().get(expectedNewSpec.specId()));
    Assert.assertEquals(spec, table.specs().get(spec.specId()));
    Assert.assertEquals(ImmutableMap.of(spec.specId(), spec, expectedNewSpec.specId(), expectedNewSpec), table.specs());
    Assert.assertNull(table.specs().get(Integer.MAX_VALUE));
  }
}
