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
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestTimestampPartitions extends TableTestBase {

  private static final Schema DATE_SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "timestamp", Types.TimestampType.withoutZone())
  );

  private static final PartitionSpec PARTITION_BY_DATE = PartitionSpec
      .builderFor(DATE_SCHEMA)
      .day("timestamp", "date")
      .build();

  private static final DataFile DATA_FILE = DataFiles.builder(PARTITION_BY_DATE)
      .withPath("/path/to/data-1.parquet")
      .withFileSizeInBytes(0)
      .withRecordCount(0)
      .withPartitionPath("date=2018-06-08")
      .build();

  @Before
  public void createTestTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    this.table = TestTables.create(tableDir, "test_date_partition", DATE_SCHEMA, PARTITION_BY_DATE);

    table.newAppend()
        .appendFile(DATA_FILE)
        .commit();
  }

  @Test
  public void testPartitionAppend() {

    long id = table.currentSnapshot().snapshotId();
    Assert.assertEquals(table.currentSnapshot().manifests().size(), 1);
    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(id),
        files(DATA_FILE),
        statuses(ManifestEntry.Status.ADDED));
  }

  private TestTables.TestTable create(Schema schema, PartitionSpec spec) {
    return TestTables.create(tableDir, "test", schema, spec);
  }

}
