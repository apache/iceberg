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

import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestDataTableScan extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestDataTableScan(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testTableScanHonorsSelect() {
    TableScan scan = table.newScan().select("id");

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        scan.schema().asStruct());
  }

  @Test
  public void testTableScanHonorsSelectWithoutCaseSensitivity() {
    TableScan scan1 = table.newScan().caseSensitive(false).select("ID");
    // order of refinements shouldn't matter
    TableScan scan2 = table.newScan().select("ID").caseSensitive(false);

    Schema expectedSchema = new Schema(required(1, "id", Types.IntegerType.get()));

    assertEquals("A tableScan.select() should prune the schema without case sensitivity",
        expectedSchema.asStruct(),
        scan1.schema().asStruct());

    assertEquals("A tableScan.select() should prune the schema regardless of scan refinement order",
        expectedSchema.asStruct(),
        scan2.schema().asStruct());
  }

}
