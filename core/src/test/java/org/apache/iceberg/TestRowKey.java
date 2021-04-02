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
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
public class TestRowKey {

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

  public TestRowKey(int formatVersion) {
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
  public void testBuildRowKey() {
    Assert.assertEquals("Should be able to build row key",
        RowKey.notIdentified(),
        RowKey.builderFor(SCHEMA).build());
  }

  @Test
  public void testDefaultRowKey() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order = SortOrder.unsorted();
    RowKey key = RowKey.notIdentified();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, order, key, formatVersion);
    Assert.assertEquals("Expected 1 partition spec", 1, table.specs().size());
    Assert.assertEquals("Expected 1 sort order", 1, table.sortOrders().size());

    RowKey actualId = table.rowKey();
    Assert.assertTrue("Row key should be default", actualId.isNotIdentified());
  }

  @Test
  public void testFreshKeys() {
    RowKey key = RowKey.builderFor(SCHEMA).addField("id").addField("data").build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA,
        PartitionSpec.unpartitioned(), SortOrder.unsorted(), key, formatVersion);

    RowKey actualKey = table.rowKey();
    Assert.assertEquals("Row key must have 2 fields", 2, actualKey.identifierFields().size());
    Assert.assertEquals("Row key must have the expected field",
        Sets.newHashSet(1, 2),
        actualKey.identifierFields().stream().map(RowKeyIdentifierField::sourceId).collect(Collectors.toSet()));
  }

  @Test
  public void testAddField() {
    AssertHelpers.assertThrows("Should not allow to add no-existing field",
        ValidationException.class, "Cannot find column with name data0 in schema",
        () -> RowKey.builderFor(SCHEMA).addField("data0").build());

    AssertHelpers.assertThrows("Should not allow to add no-existing field",
        ValidationException.class, "Cannot find column with ID 12345 in schema",
        () -> RowKey.builderFor(SCHEMA).addField(12345).build());

    AssertHelpers.assertThrows("Should not allow to add optional field",
        ValidationException.class, "because it is not a required column",
        () -> RowKey.builderFor(SCHEMA).addField("map").build());

    AssertHelpers.assertThrows("Should not allow to add optional field",
        ValidationException.class, "because it is not a required column",
        () -> RowKey.builderFor(SCHEMA).addField(14).build());

    AssertHelpers.assertThrows("Should not allow to add non-primitive field",
        ValidationException.class, "because it is not a primitive data type",
        () -> RowKey.builderFor(SCHEMA).addField("required_list").build());

    AssertHelpers.assertThrows("Should not allow to add non-primitive field",
        ValidationException.class, "because it is not a primitive data type",
        () -> RowKey.builderFor(SCHEMA).addField(17).build());
  }

  @Test
  public void testSameRowKey() {
    RowKey key1 = RowKey.builderFor(SCHEMA).addField("id").addField("data").build();
    RowKey key2 = RowKey.builderFor(SCHEMA).addField("data").addField("id").build();
    Assert.assertEquals("Row key with different order must be equal", key1, key2);
  }
}
