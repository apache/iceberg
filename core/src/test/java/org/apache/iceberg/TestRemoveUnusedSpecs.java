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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;

public class TestRemoveUnusedSpecs extends TestBase {

  @TestTemplate
  public void testRemoveAllButCurrent() {
    table
        .updateSchema()
        .addColumn("ts", Types.TimestampType.withoutZone())
        .addColumn("category", Types.StringType.get())
        .commit();
    table.updateSpec().addField("id").commit();
    table.updateSpec().addField("ts").commit();
    table.updateSpec().addField("category").commit();
    table.updateSpec().addField("data").commit();
    assertThat(table.specs().size()).as("added specs should be present").isEqualTo(5);

    PartitionSpec currentSpec = table.spec();
    table.removeUnusedSpecs().commit();

    assertThat(table.specs().size()).as("all but current spec should be removed").isEqualTo(1);
    assertThat(table.spec()).as("current spec shall not change").isEqualTo(currentSpec);
  }

  @TestTemplate
  public void testDontRemoveInUseSpecs() {
    table
        .updateSchema()
        .addColumn("ts", Types.LongType.get())
        .addColumn("category", Types.StringType.get())
        .commit();

    table.updateSpec().addField("id").commit(); // 1
    table.newAppend().appendFile(newDataFile("data_bucket=0/id=5")).commit();

    table.updateSpec().addField("ts").commit(); // 2

    table.updateSpec().addField("category").commit(); // 3
    if (formatVersion == 1) {
      table.newAppend().appendFile(newDataFile("data_bucket=0/id=5/ts=100/category=fo")).commit();
    } else {
      table
          .newRowDelta()
          .addDeletes(newDeleteFile(table.spec().specId(), "data_bucket=0/id=5/ts=100/category=fo"))
          .commit();
    }

    table.updateSpec().addField("data").commit(); // 4
    V1Assert.assertEquals("Added specs should present", 5, table.specs().size());
    V2Assert.assertEquals("Added specs should present", 5, table.specs().size());

    PartitionSpec currentSpec = table.spec();
    table.removeUnusedSpecs().commit();

    V1Assert.assertEquals(
        "Missing required spec", ImmutableSet.of(1, 3, 4), table.specs().keySet());
    V2Assert.assertEquals(
        "Missing required spec", ImmutableSet.of(1, 3, 4), table.specs().keySet());
    V1Assert.assertEquals("Current spec shall not change", currentSpec, table.spec());
    V2Assert.assertEquals("Current spec shall not change", currentSpec, table.spec());
  }
}
