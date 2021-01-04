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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestPrimaryKey {

  private final int formatVersion;

  private static final Schema SCHEMA = new Schema(
      required(10, "id", Types.IntegerType.get()),
      required(11, "data", Types.StringType.get()),
      optional(12, "s", Types.StructType.of(
          required(13, "id", Types.IntegerType.get())
      )),
      optional(14, "map", Types.MapType.ofOptional(
          15, 16, Types.IntegerType.get(), Types.IntegerType.get()
      )),
      required(17, "required_list", Types.ListType.ofOptional(18, Types.StringType.get()))
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {1},
        new Object[] {2}
    );
  }

  public TestPrimaryKey(int formatVersion) {
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
  public void testReservedPrimaryKey() {
    Assert.assertEquals("Should be able to build non-primary key",
        PrimaryKey.nonPrimaryKey(),
        PrimaryKey.builderFor(SCHEMA).withKeyId(0).build());

    AssertHelpers.assertThrows("Should not allow primary key ID 0",
        IllegalArgumentException.class, "Primary key ID 0 is reserved for non-primary key",
        () -> PrimaryKey.builderFor(SCHEMA).addField("id").withKeyId(0).build());

    AssertHelpers.assertThrows("Should not allow non-primary key with arbitrary IDs",
        IllegalArgumentException.class, "Non-primary key ID must be 0",
        () -> PrimaryKey.builderFor(SCHEMA).withKeyId(1).build());
  }

  @Test
  public void testDefaultPrimaryKey() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.unsorted();
    PrimaryKey key = PrimaryKey.nonPrimaryKey();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, key, formatVersion);
    Assert.assertEquals("Expected 1 partition spec", 1, table.specs().size());
    Assert.assertEquals("Expected 1 sort order", 1, table.sortOrders().size());
    Assert.assertEquals("Expected 1 primary key", 1, table.primaryKeys().size());

    PrimaryKey actualKey = table.primaryKey();
    Assert.assertEquals("Primary key ID must match", 0, actualKey.keyId());
    Assert.assertTrue("Primary key is non-primary key", actualKey.isNonPrimaryKey());
  }

  @Test
  public void testFreshIds() {
    PrimaryKey key = PrimaryKey.builderFor(SCHEMA)
        .withKeyId(1)
        .addField("id")
        .addField("data")
        .build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA,
        PartitionSpec.unpartitioned(), SortOrder.unsorted(), key, formatVersion);

    Assert.assertEquals("Expected 1 primary key", 1, table.primaryKeys().size());
    Assert.assertTrue("Primary key ID must be fresh", table.primaryKeys().containsKey(1));

    PrimaryKey actualKey = table.primaryKey();
    Assert.assertEquals("Primary key must be fresh", TableMetadata.INITIAL_PRIMARY_KEY_ID, actualKey.keyId());
    Assert.assertEquals("Primary key must have 2 fields", 2, actualKey.sourceIds().size());
    Assert.assertEquals("Field id must be fresh", Integer.valueOf(1), actualKey.sourceIds().get(0));
    Assert.assertEquals("Field id must be fresh", Integer.valueOf(2), actualKey.sourceIds().get(1));
  }

  @Test
  public void testAddField() {
    AssertHelpers.assertThrows("Should not allow to add no-existing field",
        NullPointerException.class, "Cannot find source column: data0",
        () -> PrimaryKey.builderFor(SCHEMA).addField("data0").build());

    AssertHelpers.assertThrows("Should not allow to add no-existing field",
        NullPointerException.class, "Cannot find source column: 2147483647",
        () -> PrimaryKey.builderFor(SCHEMA).addField(2147483647).build());

    AssertHelpers.assertThrows("Should not allow to add optional field",
        IllegalArgumentException.class, "Cannot add optional source field to primary key: map",
        () -> PrimaryKey.builderFor(SCHEMA).addField("map").build());

    AssertHelpers.assertThrows("Should not allow to add optional field",
        IllegalArgumentException.class, "Cannot add optional source field to primary key: 14",
        () -> PrimaryKey.builderFor(SCHEMA).addField(14).build());

    AssertHelpers.assertThrows("Should not allow to add non-primitive field",
        ValidationException.class, "Cannot add non-primitive field: list<string>",
        () -> PrimaryKey.builderFor(SCHEMA).addField("required_list").build());

    AssertHelpers.assertThrows("Should not allow to add non-primitive field",
        ValidationException.class, "Cannot add non-primitive field: list<string>",
        () -> PrimaryKey.builderFor(SCHEMA).addField(17).build());
  }

  @Test
  public void testSamePrimaryKey() {
    PrimaryKey pk1 = PrimaryKey.builderFor(SCHEMA)
        .withKeyId(1)
        .addField("id")
        .addField("data")
        .withEnforceUniqueness(false)
        .build();
    PrimaryKey pk2 = PrimaryKey.builderFor(SCHEMA)
        .withKeyId(2)
        .addField("id")
        .addField("data")
        .build();
    PrimaryKey pk3 = PrimaryKey.builderFor(SCHEMA)
        .withKeyId(3)
        .addField("data")
        .addField("id")
        .build();
    PrimaryKey pk4 = PrimaryKey.builderFor(SCHEMA)
        .withKeyId(1)
        .addField("id")
        .addField("data")
        .withEnforceUniqueness(true)
        .build();
    PrimaryKey pk5 = PrimaryKey.builderFor(SCHEMA)
        .withKeyId(1)
        .addField("id")
        .addField("data")
        .withEnforceUniqueness(false)
        .build();

    Assert.assertNotEquals("Primary key must not be equal.", pk1, pk2);
    Assert.assertTrue("Primary key must be equivalent", pk1.samePrimaryKey(pk2));
    Assert.assertTrue("Primary key must be equivalent", pk2.samePrimaryKey(pk1));

    Assert.assertNotEquals("Primary key must not be equal", pk1, pk3);
    Assert.assertFalse("Primary key must not be equivalent", pk1.samePrimaryKey(pk3));
    Assert.assertFalse("Primary key must not be equivalent", pk3.samePrimaryKey(pk1));

    Assert.assertNotEquals("Primary key must not be equal", pk1, pk4);
    Assert.assertFalse("Primary key must not be equivalent", pk1.samePrimaryKey(pk4));
    Assert.assertFalse("Primary key must not be equivalent", pk4.samePrimaryKey(pk1));

    Assert.assertEquals("Primary key must be equal", pk1, pk5);
    Assert.assertTrue("Primary key must be equivalent", pk1.samePrimaryKey(pk5));
    Assert.assertTrue("Primary key must be equivalent", pk5.samePrimaryKey(pk1));
  }
}
