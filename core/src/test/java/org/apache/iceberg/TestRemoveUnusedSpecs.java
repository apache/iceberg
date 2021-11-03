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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRemoveUnusedSpecs extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestRemoveUnusedSpecs(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testRemoveAllButCurrent() {
    table.updateSchema()
        .addColumn("ts", Types.TimestampType.withoutZone())
        .addColumn("category", Types.StringType.get())
        .commit();
    table.updateSpec().addField("id").commit();
    table.updateSpec().addField("ts").commit();
    table.updateSpec().addField("category").commit();
    table.updateSpec().addField("data").commit();
    Assert.assertEquals(5, table.specs().size());

    PartitionSpec currentSpec = table.spec();
    table.removeUnusedSpecs().commit();

    Assert.assertEquals(1, table.specs().size());
    Assert.assertEquals(currentSpec, table.spec());
  }

  @Test
  public void testDontRemoveInUseSpecsV2() {
    Assume.assumeTrue("V2", formatVersion == 2);

    table.updateSchema()
        .addColumn("ts", Types.LongType.get())
        .addColumn("category", Types.StringType.get())
        .commit();

    table.updateSpec().addField("id").commit(); // 1
    table.newAppend().appendFile(newDataFile("data_bucket=0/id=5")).commit();

    table.updateSpec().addField("ts").commit(); // 2

    table.updateSpec().addField("category").commit(); // 3
    table.newRowDelta()
        .addDeletes(newDeleteFile(table.spec().specId(), "data_bucket=0/id=5/ts=100/category=fo"))
        .commit();

    table.updateSpec().addField("data").commit(); // 4
    Assert.assertEquals(5, table.specs().size());

    PartitionSpec currentSpec = table.spec();
    table.removeUnusedSpecs().commit();

    Assert.assertEquals("Missing required spec", ImmutableSet.of(1, 3, 4), table.specs().keySet());
    Assert.assertEquals(currentSpec, table.spec());
  }

  @Test
  public void testDontRemoveInUseSpecs() {
    Assume.assumeTrue("V2", formatVersion == 2);

    table.updateSchema()
        .addColumn("ts", Types.LongType.get())
        .addColumn("category", Types.StringType.get())
        .commit();

    table.updateSpec().addField("id").commit(); // 1
    table.newAppend().appendFile(newDataFile("data_bucket=0/id=5")).commit();

    table.updateSpec().addField("ts").commit(); // 2

    table.updateSpec().addField("category").commit(); // 3
    table.newAppend().appendFile(newDataFile("data_bucket=0/id=5/ts=100/category=fo")).commit();

    table.updateSpec().addField("data").commit(); // 4
    Assert.assertEquals(5, table.specs().size());

    PartitionSpec currentSpec = table.spec();
    table.removeUnusedSpecs().commit();

    Assert.assertEquals("Missing required spec", ImmutableSet.of(1, 3, 4), table.specs().keySet());
    Assert.assertEquals(currentSpec, table.spec());
  }
}
