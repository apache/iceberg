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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestReplaceTransaction extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testReplaceTransactionWithCustomSortOrder() {
    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    SortOrder newSortOrder = SortOrder.builderFor(schema).asc("id", NULLS_FIRST).build();

    Map<String, String> props = Maps.newHashMap();
    Transaction replace =
        TestTables.beginReplace(tableDir, "test", schema, unpartitioned(), newSortOrder, props);
    replace.commitTransaction();

    table.refresh();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.schema().asStruct()).isEqualTo(schema.asStruct());

    PartitionSpec v2Expected = PartitionSpec.builderFor(table.schema()).withSpecId(1).build();
    V2Assert.assertEquals("Table should have an unpartitioned spec", v2Expected, table.spec());

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(table.schema())
            .alwaysNull("data", "data_bucket")
            .withSpecId(1)
            .build();
    V1Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());

    assertThat(table.sortOrders()).hasSize(2);
    SortOrder sortOrder = table.sortOrder();
    assertThat(sortOrder.orderId()).isEqualTo(1);
    assertThat(sortOrder.fields()).hasSize(1);
    assertThat(sortOrder.fields().get(0).direction()).isEqualTo(ASC);
    assertThat(sortOrder.fields().get(0).nullOrder()).isEqualTo(NULLS_FIRST);
    Transform<?, ?> transform = Transforms.identity();
    assertThat(sortOrder.fields().get(0).transform()).isEqualTo(transform);
  }

  @TestTemplate
  public void testReplaceTransaction() {
    Schema newSchema =
        new Schema(
            required(4, "id", Types.IntegerType.get()),
            required(5, "data", Types.StringType.get()));

    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", newSchema, unpartitioned());
    replace.commitTransaction();

    table.refresh();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.schema().asStruct()).isEqualTo(schema.asStruct());

    PartitionSpec v2Expected = PartitionSpec.builderFor(table.schema()).withSpecId(1).build();
    V2Assert.assertEquals("Table should have an unpartitioned spec", v2Expected, table.spec());

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(table.schema())
            .alwaysNull("data", "data_bucket")
            .withSpecId(1)
            .build();
    V1Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());

    assertThat(table.sortOrders()).hasSize(1);
    assertThat(table.sortOrder().orderId()).isEqualTo(0);
    assertThat(table.sortOrder().isUnsorted()).isTrue();
  }

  @TestTemplate
  public void testReplaceWithIncompatibleSchemaUpdate() {
    assumeThat(formatVersion)
        .as("Fails early for v1 tables because partition spec cannot drop a field")
        .isEqualTo(2);

    Schema newSchema = new Schema(required(4, "obj_id", Types.IntegerType.get()));

    Snapshot start = table.currentSnapshot();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", newSchema, unpartitioned());
    replace.commitTransaction();

    table.refresh();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.schema().asStruct())
        .isEqualTo(new Schema(required(3, "obj_id", Types.IntegerType.get())).asStruct());
  }

  @TestTemplate
  public void testReplaceWithNewPartitionSpec() {
    PartitionSpec newSpec = PartitionSpec.unpartitioned();

    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), newSpec);
    replace.commitTransaction();

    table.refresh();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.schema().asStruct()).isEqualTo(schema.asStruct());

    PartitionSpec v2Expected = PartitionSpec.builderFor(table.schema()).withSpecId(1).build();
    V2Assert.assertEquals("Table should have an unpartitioned spec", v2Expected, table.spec());

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(table.schema())
            .alwaysNull("data", "data_bucket")
            .withSpecId(1)
            .build();
    V1Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());
  }

  @TestTemplate
  public void testReplaceWithNewData() {
    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    replace.commitTransaction();

    table.refresh();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot()).isNotNull();
    assertThat(table.schema().asStruct()).isEqualTo(schema.asStruct());

    validateSnapshot(null, table.currentSnapshot(), FILE_B, FILE_C, FILE_D);
  }

  @TestTemplate
  public void testReplaceDetectsUncommittedChangeOnCommit() {
    assertThat(version()).isEqualTo(0);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace
        .newAppend() // not committed
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D);

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot commit transaction: last operation has not committed");

    assertThat(version()).isEqualTo(0);
  }

  @TestTemplate
  public void testReplaceDetectsUncommittedChangeOnTableCommit() {
    assertThat(version()).isEqualTo(0);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace
        .table()
        .newAppend() // not committed
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D);

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot commit transaction: last operation has not committed");

    assertThat(version()).isEqualTo(0);
  }

  @TestTemplate
  public void testReplaceTransactionRetry() {
    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // trigger eventual transaction retry
    ((TestTables.TestTableOperations) ((BaseTransaction) replace).ops()).failCommits(1);

    replace.commitTransaction();

    table.refresh();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot()).isNotNull();
    assertThat(table.schema().asStruct()).isEqualTo(schema.asStruct());

    validateSnapshot(null, table.currentSnapshot(), FILE_B, FILE_C, FILE_D);
  }

  @TestTemplate
  public void testReplaceTransactionConflict() {
    Snapshot start = table.currentSnapshot();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1);

    validateSnapshot(start, table.currentSnapshot(), FILE_A);
    Set<File> manifests = Sets.newHashSet(listManifestFiles());

    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());

    replace.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // keep failing to trigger eventual transaction failure
    ((TestTables.TestTableOperations) ((BaseTransaction) replace).ops()).failCommits(100);

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    assertThat(version()).isEqualTo(1);

    table.refresh();

    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    assertThat(listManifestFiles()).containsExactlyElementsOf(manifests);
  }

  @TestTemplate
  public void testReplaceToCreateAndAppend() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    // this table doesn't exist.
    Transaction replace = TestTables.beginReplace(tableDir, "test_append", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_append")).isNull();
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    assertThat(replace.table()).isInstanceOf(BaseTransaction.TransactionTable.class);

    replace.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(TestTables.readMetadata("test_append")).isNull();
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    replace.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_append");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_append")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).hasSize(1);

    assertThat(meta.schema().asStruct()).isEqualTo(assignFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).hasSize(1);

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  @TestTemplate
  public void testReplaceTransactionWithUnknownState() {
    Schema newSchema =
        new Schema(
            required(4, "id", Types.IntegerType.get()),
            required(5, "data", Types.StringType.get()));

    Snapshot start = table.currentSnapshot();
    Schema schema = table.schema();

    table.newAppend().appendFile(FILE_A).commit();

    assertThat(version()).isEqualTo(1L);
    validateSnapshot(start, table.currentSnapshot(), FILE_A);

    TestTables.TestTableOperations ops =
        TestTables.opsWithCommitSucceedButStateUnknown(tableDir, "test");
    Transaction replace =
        TestTables.beginReplace(
            tableDir,
            "test",
            newSchema,
            unpartitioned(),
            SortOrder.unsorted(),
            ImmutableMap.of(),
            ops);

    replace.newAppend().appendFile(FILE_B).commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("datacenter on fire");

    table.refresh();

    assertThat(version()).isEqualTo(2L);
    assertThat(table.currentSnapshot()).isNotNull();
    assertThat(table.schema().asStruct()).isEqualTo(schema.asStruct());
    assertThat(countAllMetadataFiles(tableDir)).isEqualTo(4);
    validateSnapshot(null, table.currentSnapshot(), FILE_B);
  }

  @TestTemplate
  public void testCreateTransactionWithUnknownState() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    // this table doesn't exist.
    TestTables.TestTableOperations ops =
        TestTables.opsWithCommitSucceedButStateUnknown(tableDir, "test_append");
    Transaction replace =
        TestTables.beginReplace(
            tableDir,
            "test_append",
            SCHEMA,
            unpartitioned(),
            SortOrder.unsorted(),
            ImmutableMap.of(),
            ops);

    assertThat(TestTables.readMetadata("test_append")).isNull();
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    assertThat(replace.table()).isInstanceOf(BaseTransaction.TransactionTable.class);

    replace.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(TestTables.readMetadata("test_append")).isNull();
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("datacenter on fire");

    TableMetadata meta = TestTables.readMetadata("test_append");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_append")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).hasSize(1);
    assertThat(countAllMetadataFiles(tableDir)).isEqualTo(2);
    assertThat(meta.schema().asStruct()).isEqualTo(assignFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).hasSize(1);

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  private static Schema assignFreshIds(Schema schema) {
    AtomicInteger lastColumnId = new AtomicInteger(0);
    return TypeUtil.assignFreshIds(schema, lastColumnId::incrementAndGet);
  }
}
