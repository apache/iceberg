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

import com.google.common.collect.Iterables;
import java.io.IOException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestPartitionPath {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.TimestampType.withoutZone()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testPartitionFromPathForTimestamp() throws IOException {
    final PartitionSpec aPartitionSpec = PartitionSpec
        .builderFor(SCHEMA)
        .add(3, "y", "year")
        .add(3, "m", "month")
        .add(3, "d", "day")
        .add(3, "h", "hour")
        .build();

    TestTables.TestTable testTable = TestTables.create(temp.newFolder(), "table", SCHEMA, aPartitionSpec);

    DataFile file = DataFiles
        .builder(aPartitionSpec)
        .withPath("/path/to/data.parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(12)
        // TODO - test fails should we not preserve order of fields as defined in the partition spec, should it?
        .withPartitionPath("y=2018/m=2018-06/d=2018-06-08/h=2018-06-08-12")
        .build();

    testTable.newAppend()
        .appendFile(file)
        .commit();

    Assert.assertEquals(1, testTable.currentSnapshot().manifests().size());

    Object withZone = Literal.of("2018-06-08T12:01:01.000Z").to(Types.TimestampType.withZone()).value();
    TableScan tableScanWithZone = testTable.newScan().filter(Expressions.equal("date", withZone));
    Assert.assertEquals(1, Iterables.size(tableScanWithZone.planFiles()));

    Object withoutZone = Literal.of("2018-06-08T12:01:01.000000").to(Types.TimestampType.withoutZone()).value();
    TableScan tableScanWithoutZone = testTable.newScan().filter(Expressions.equal("date", withoutZone));
    Assert.assertEquals(1, Iterables.size(tableScanWithoutZone.planFiles()));
  }

  @Test
  public void testPartitionFromPathForIdentity() throws IOException {
    // TODO - found out that it is expected that the path partitions order is same as partition spec definition order
    final PartitionSpec aPartitionSpec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("id")
        .build();

    TestTables.TestTable testTable = TestTables.create(temp.newFolder(), "table", SCHEMA, aPartitionSpec);

    DataFile file = DataFiles
        .builder(aPartitionSpec)
        .withPath("/path/to/data.parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(12)
        .withPartitionPath("id=132523123")
        .build();

    testTable.newAppend()
        .appendFile(file)
        .commit();

    Assert.assertEquals(1, testTable.currentSnapshot().manifests().size());

    Object markerWithZone = Literal.of(132523123).to(Types.LongType.get()).value();
    UnboundPredicate<Object> date = Expressions.equal("id", markerWithZone);

    TableScan tableScan = testTable.newScan().filter(date);
    Assert.assertEquals(1, Iterables.size(tableScan.planFiles()));
  }
}
