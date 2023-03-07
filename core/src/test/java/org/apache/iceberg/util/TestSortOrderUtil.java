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

import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSortOrderUtil {

  // column ids will be reassigned during table creation
  private static final Schema SCHEMA =
      new Schema(
          required(10, "id", Types.IntegerType.get()),
          required(11, "data", Types.StringType.get()),
          required(12, "ts", Types.TimestampType.withZone()),
          required(13, "category", Types.StringType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  @Before
  public void setupTableDir() throws IOException {
    this.tableDir = temp.newFolder();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testEmptySpecsV1() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.builderFor(SCHEMA).withOrderId(1).asc("id", NULLS_LAST).build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, 1);

    // pass PartitionSpec.unpartitioned() on purpose as it has an empty schema
    SortOrder actualOrder = SortOrderUtil.buildSortOrder(table.schema(), spec, table.sortOrder());

    Assert.assertEquals("Order ID must be fresh", 1, actualOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, actualOrder.fields().size());
    Assert.assertEquals("Field id must be fresh", 1, actualOrder.fields().get(0).sourceId());
    Assert.assertEquals("Direction must match", ASC, actualOrder.fields().get(0).direction());
    Assert.assertEquals(
        "Null order must match", NULLS_LAST, actualOrder.fields().get(0).nullOrder());
  }

  @Test
  public void testEmptySpecsV2() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.builderFor(SCHEMA).withOrderId(1).asc("id", NULLS_LAST).build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, 2);

    // pass PartitionSpec.unpartitioned() on purpose as it has an empty schema
    SortOrder actualOrder = SortOrderUtil.buildSortOrder(table.schema(), spec, table.sortOrder());

    Assert.assertEquals("Order ID must be fresh", 1, actualOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, actualOrder.fields().size());
    Assert.assertEquals("Field id must be fresh", 1, actualOrder.fields().get(0).sourceId());
    Assert.assertEquals("Direction must match", ASC, actualOrder.fields().get(0).direction());
    Assert.assertEquals(
        "Null order must match", NULLS_LAST, actualOrder.fields().get(0).nullOrder());
  }

  @Test
  public void testSortOrderClusteringNoPartitionFields() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("category").build();
    SortOrder order = SortOrder.builderFor(SCHEMA).withOrderId(1).desc("id").build();

    SortOrder expected =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc(Expressions.day("ts"))
            .asc("category")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringAllPartitionFields() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("category").build();
    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc(Expressions.day("ts"))
            .asc("category")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should leave the order unchanged",
        order,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringAllPartitionFieldsReordered() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();
    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc(Expressions.day("ts"))
            .asc("category")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should leave the order unchanged",
        order,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringSomePartitionFields() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();
    SortOrder order =
        SortOrder.builderFor(SCHEMA).withOrderId(1).asc("category").desc("id").build();

    SortOrder expected =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc(Expressions.day("ts"))
            .asc("category")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringSatisfiedPartitionLast() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();
    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc("category")
            .asc("ts") // satisfies the ordering of days(ts)
            .desc("id")
            .build();

    SortOrder expected =
        SortOrder.builderFor(SCHEMA).withOrderId(1).asc("category").asc("ts").desc("id").build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringSatisfiedPartitionFirst() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("category").build();
    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc("ts") // satisfies the ordering of days(ts)
            .asc("category")
            .desc("id")
            .build();

    SortOrder expected =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc("category") // prefix is added, the rest of the sort order stays the same
            .asc("ts")
            .asc("category")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringSatisfiedPartitionFields() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("category").build();

    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc("ts") // satisfies the ordering of days(ts)
            .asc("category")
            .desc("id")
            .build();

    SortOrder expected =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(1)
            .asc("category") // prefix is added, the rest of the sort order stays the same
            .asc("ts")
            .asc("category")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(SCHEMA, spec, order));
  }

  @Test
  public void testSortOrderClusteringWithRedundantPartitionFields() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("category").build();

    // Specs with redundant time fields can't be constructed directly and have to use
    // UpdatePartitionSpec
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, SortOrder.unsorted(), 2);
    table.updateSpec().addField(Expressions.hour("ts")).commit();
    PartitionSpec updatedSpec = table.spec();

    SortOrder order =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category")
            .asc("ts") // satisfies the ordering of days(ts) and hours(ts)
            .desc("id")
            .build();

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category")
            .asc("ts")
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(table.schema(), updatedSpec, order));
  }

  @Test
  public void testSortOrderClusteringWithRedundantPartitionFieldsMissing() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").identity("category").build();

    // Specs with redundant time fields can't be constructed directly and have to use
    // UpdatePartitionSpec
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, SortOrder.unsorted(), 1);
    table
        .updateSpec()
        .removeField("ts_day") // introduce a void transform
        .addField(Expressions.hour("ts"))
        .commit();
    PartitionSpec updatedSpec = table.spec();

    SortOrder order = SortOrder.builderFor(table.schema()).withOrderId(1).desc("id").build();

    SortOrder expected =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category")
            .asc(Expressions.hour("ts"))
            .desc("id")
            .build();

    Assert.assertEquals(
        "Should add spec fields as prefix",
        expected,
        SortOrderUtil.buildSortOrder(table.schema(), updatedSpec, order));
  }
}
