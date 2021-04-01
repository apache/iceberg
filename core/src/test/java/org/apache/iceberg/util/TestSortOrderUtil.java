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

package org.apache.iceberg.util;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSortOrderUtil {

  // column ids will be reassigned during table creation
  private static final Schema SCHEMA = new Schema(
      required(10, "id", Types.IntegerType.get()),
      required(11, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  private final int formatVersion;

  public TestSortOrderUtil(int formatVersion) {
    this.formatVersion = formatVersion;
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
  public void testEmptySpecs() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .withOrderId(1)
        .asc("id", NULLS_LAST)
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    // pass PartitionSpec.unpartitioned() on purpose as it has an empty schema
    SortOrder actualOrder = SortOrderUtil.buildSortOrder(table.schema(), spec, table.sortOrder());

    Assert.assertEquals("Order ID must be fresh", 1, actualOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, actualOrder.fields().size());
    Assert.assertEquals("Field id must be fresh", 1, actualOrder.fields().get(0).sourceId());
    Assert.assertEquals("Direction must match", ASC, actualOrder.fields().get(0).direction());
    Assert.assertEquals("Null order must match", NULLS_LAST, actualOrder.fields().get(0).nullOrder());
  }
}
