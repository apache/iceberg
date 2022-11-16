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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestPartitioning {

  private static final int V1_FORMAT_VERSION = 1;
  private static final int V2_FORMAT_VERSION = 2;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "category", Types.StringType.get()));
  private static final PartitionSpec BY_DATA_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();
  private static final PartitionSpec BY_CATEGORY_DATA_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("category").identity("data").build();
  private static final PartitionSpec BY_DATA_CATEGORY_BUCKET_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").bucket("category", 8).build();

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
  public void testPartitionTypeWithSpecEvolutionInV1Tables() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V1_FORMAT_VERSION);

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "data", Types.StringType.get()),
            NestedField.optional(1001, "category_bucket_8", Types.IntegerType.get()));
    StructType actualType = Partitioning.partitionType(table);
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testPartitionTypeWithSpecEvolutionInV2Tables() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V2_FORMAT_VERSION);

    table.updateSpec().removeField("data").addField("category").commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "data", Types.StringType.get()),
            NestedField.optional(1001, "category", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testPartitionTypeWithRenamesInV1Table() {
    PartitionSpec initialSpec = PartitionSpec.builderFor(SCHEMA).identity("data", "p1").build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, initialSpec, V1_FORMAT_VERSION);

    table.updateSpec().addField("category").commit();

    table.updateSpec().renameField("p1", "p2").commit();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p2", Types.StringType.get()),
            NestedField.optional(1001, "category", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testPartitionTypeWithAddingBackSamePartitionFieldInV1Table() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V1_FORMAT_VERSION);

    table.updateSpec().removeField("data").commit();

    table.updateSpec().addField("data").commit();

    // in v1, we use void transforms instead of dropping partition fields
    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "data_1000", Types.StringType.get()),
            NestedField.optional(1001, "data", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testPartitionTypeWithAddingBackSamePartitionFieldInV2Table() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V2_FORMAT_VERSION);

    table.updateSpec().removeField("data").commit();

    table.updateSpec().addField("data").commit();

    // in v2, we should be able to reuse the original partition spec
    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testPartitionTypeWithIncompatibleSpecEvolution() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V1_FORMAT_VERSION);

    PartitionSpec newSpec = PartitionSpec.builderFor(table.schema()).identity("category").build();

    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata current = ops.current();
    ops.commit(current, current.updatePartitionSpec(newSpec));

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    AssertHelpers.assertThrows(
        "Should complain about incompatible specs",
        ValidationException.class,
        "Conflicting partition fields",
        () -> Partitioning.partitionType(table));
  }

  @Test
  public void testGroupingKeyTypeWithSpecEvolutionInV1Tables() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V1_FORMAT_VERSION);

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithSpecEvolutionInV2Tables() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V2_FORMAT_VERSION);

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithDroppedPartitionFieldInV1Tables() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", SCHEMA, BY_DATA_CATEGORY_BUCKET_SPEC, V1_FORMAT_VERSION);

    table.updateSpec().removeField(Expressions.bucket("category", 8)).commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithDroppedPartitionFieldInV2Tables() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", SCHEMA, BY_DATA_CATEGORY_BUCKET_SPEC, V2_FORMAT_VERSION);

    table.updateSpec().removeField(Expressions.bucket("category", 8)).commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithRenamesInV1Table() {
    PartitionSpec initialSpec = PartitionSpec.builderFor(SCHEMA).identity("data", "p1").build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, initialSpec, V1_FORMAT_VERSION);

    table.updateSpec().addField("category").commit();

    table.updateSpec().renameField("p1", "p2").commit();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "p2", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithRenamesInV2Table() {
    PartitionSpec initialSpec = PartitionSpec.builderFor(SCHEMA).identity("data", "p1").build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, initialSpec, V2_FORMAT_VERSION);

    table.updateSpec().addField("category").commit();

    table.updateSpec().renameField("p1", "p2").commit();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "p2", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithEvolvedIntoUnpartitionedSpecV1Table() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V1_FORMAT_VERSION);

    table.updateSpec().removeField("data").commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType = StructType.of();
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithEvolvedIntoUnpartitionedSpecV2Table() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V2_FORMAT_VERSION);

    table.updateSpec().removeField("data").commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType = StructType.of();
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithAddingBackSamePartitionFieldInV1Table() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_CATEGORY_DATA_SPEC, V1_FORMAT_VERSION);

    table.updateSpec().removeField("data").commit();

    table.updateSpec().addField("data").commit();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "category", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithAddingBackSamePartitionFieldInV2Table() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_CATEGORY_DATA_SPEC, V2_FORMAT_VERSION);

    table.updateSpec().removeField("data").commit();

    table.updateSpec().addField("data").commit();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "category", Types.StringType.get()));
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithOnlyUnpartitionedSpec() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), V1_FORMAT_VERSION);

    Assert.assertEquals("Should have 1 spec", 1, table.specs().size());

    StructType expectedType = StructType.of();
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithEvolvedUnpartitionedSpec() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), V1_FORMAT_VERSION);

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    StructType expectedType = StructType.of();
    StructType actualType = Partitioning.groupingKeyType(table.specs().values());
    Assert.assertEquals("Types must match", expectedType, actualType);
  }

  @Test
  public void testGroupingKeyTypeWithIncompatibleSpecEvolution() {
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, BY_DATA_SPEC, V1_FORMAT_VERSION);

    PartitionSpec newSpec = PartitionSpec.builderFor(table.schema()).identity("category").build();

    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata current = ops.current();
    ops.commit(current, current.updatePartitionSpec(newSpec));

    Assert.assertEquals("Should have 2 specs", 2, table.specs().size());

    AssertHelpers.assertThrows(
        "Should complain about incompatible specs",
        ValidationException.class,
        "Conflicting partition fields",
        () -> Partitioning.groupingKeyType(table.specs().values()));
  }
}
