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

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;

public class TestRemovePartitionSpecs extends TestBase {

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
    assertThat(table.specs().size()).as("Added specs should be present").isEqualTo(5);

    PartitionSpec currentSpec = table.spec();
    table.maintenance().removeUnusedSpecs().commit();

    assertThat(table.specs().size()).as("All but current spec should be removed").isEqualTo(1);
    assertThat(table.spec()).as("Current spec shall not change").isEqualTo(currentSpec);
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
    assertThat(table.specs()).size().as("Added specs should be present").isEqualTo(5);

    PartitionSpec currentSpec = table.spec();
    table.maintenance().removeUnusedSpecs().commit();
    assertThat(table.specs().keySet()).as("Unused specs are removed").containsExactly(1, 3, 4);
    assertThat(table.spec()).as("Current spec shall not change").isEqualTo(currentSpec);
  }

  @TestTemplate
  public void testRemoveUnpartitionedSpec() {
    // clean it first to reset to unpartitioned
    cleanupTables();
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-0.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(file).commit();

    table.updateSpec().addField("data_bucket", Expressions.bucket("data", 16)).commit();

    // removeUnusedPartitionSpec shall not remove the unpartitioned spec
    table.maintenance().removeUnusedSpecs().commit();
    assertThat(table.specs().keySet()).as("unpartitioned spec is still used").containsExactly(0, 1);

    table.newDelete().deleteFile(file).commit();
    DataFile bucketFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-0.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(100)
            .withPartitionPath("data_bucket=0")
            .build();
    table.newAppend().appendFile(bucketFile).commit();
    table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();

    table.maintenance().removeUnusedSpecs().commit();
    assertThat(table.specs().keySet())
        .as("unpartitioned spec should be removed")
        .containsExactly(1);

    table.updateSpec().removeField("data_bucket").commit();
    assertThat(table.spec().isUnpartitioned()).as("Should equal to unpartitioned").isTrue();

    int unpartitionedFieldsSize = formatVersion == 1 ? 1 : 0;
    assertThat(table.spec().fields().size())
        .as("Should have one void transform for v1 and empty for v2")
        .isEqualTo(unpartitionedFieldsSize);
    assertThat(table.spec().specId())
        .as("unpartitioned is evolved to use a new SpecId")
        .isEqualTo(2);
  }
}
