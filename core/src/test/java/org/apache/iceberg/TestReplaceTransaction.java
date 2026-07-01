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

  /**
   * Verifies that a replace transaction rebases onto refreshed metadata when a concurrent commit
   * lands BEFORE the replace's first commit attempt -- so no {@link CommitFailedException} is
   * thrown and the retry loop is never exercised.
   *
   * <p>A replace is last-writer-wins on the current schema, spec, and data, but it is not a
   * drop-and-recreate: the table keeps its history. When {@code commitReplaceTransaction} observes
   * that the base advanced (a concurrent commit), it rebuilds the replacement on the refreshed base
   * and replays the staged updates. Rebuilding carries forward the concurrent commit's snapshots so
   * history is preserved, and {@link SnapshotProducer#apply()} re-derives the replacement head's
   * sequence number from the refreshed base ({@code base.nextSequenceNumber()}). The committed head
   * therefore receives a STRICTLY greater sequence number than the concurrent commit and the
   * table's {@code lastSequenceNumber} advances, preserving the monotonicity that {@link
   * TableMetadata.Builder} otherwise guarantees across a replace.
   */
  @TestTemplate
  public void testReplaceTransactionRebasesOntoConcurrentCommit() {
    // Sequence numbers are only meaningful in format v2+.
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    // Use random snapshot ids so the staged replacement snapshot and the concurrent commit's
    // snapshot do not collide on a sequential id (which would send the rebased commit down the
    // rollback path in TestTableOperations). Production catalogs assign random snapshot ids.
    table.updateProperties().set("random-snapshot-ids", "true").commit();

    // Seed: one append -> seq 1, lastSequenceNumber 1.
    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1L);

    // Begin the replace (MV full refresh / CREATE OR REPLACE). Base is captured at lastSeq 1.
    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());
    // Staged replacement head, assigned seq 2 from the stage-time (seq-1) base. This value will be
    // re-derived when the replace rebases onto the refreshed base at commit time.
    replace.newAppend().appendFile(FILE_B).commit();

    // Concurrent writer (e.g. compaction) commits against the SAME seq-1 base -> also gets seq 2.
    // This lands BEFORE replace.commitTransaction(), so the replace's first refresh() observes it
    // and the commit succeeds on the first attempt with no retry.
    table.newAppend().appendFile(FILE_C).commit();
    table.refresh();
    long concurrentSeq = table.ops().current().lastSequenceNumber();
    assertThat(concurrentSeq).isEqualTo(2L);
    int versionBeforeReplace = version();

    // No failCommits injection: refresh() reads the up-to-date (concurrent) version token, base is
    // advanced to the concurrent metadata, the replace rebuilds on it, and the CAS matches ->
    // first-attempt success, no retry.
    replace.commitTransaction();
    table.refresh();

    // Exactly one additional successful commit (no retry).
    assertThat(version()).isEqualTo(versionBeforeReplace + 1);

    TableMetadata after = table.ops().current();
    Snapshot newHead = after.currentSnapshot();

    // The replacement head receives a STRICTLY greater sequence number than the concurrent commit,
    // because it was re-derived from the refreshed (seq-2) base the replace actually committed
    // against rather than the stale stage-time (seq-1) base.
    assertThat(newHead.sequenceNumber()).isGreaterThan(concurrentSeq); // == 3
    // The table's monotonic sequence counter advances past the concurrent commit.
    assertThat(after.lastSequenceNumber()).isGreaterThan(concurrentSeq); // == 3

    // History is preserved: the concurrent commit's snapshot remains in the table's snapshot list,
    // so the replace did not erase it "as though the concurrent commit never happened".
    assertThat(after.snapshots())
        .as("Concurrent commit's snapshot must be preserved in history after replace")
        .anyMatch(snapshot -> snapshot.sequenceNumber() == concurrentSeq);
  }

  /**
   * Mirrors the concurrent-schema example from the REPLACE TABLE concurrency discussion: a
   * concurrent commit evolves the schema (adding a new schema id and making it current) after a
   * replace transaction has been staged. A replace is last-writer-wins on the current schema, so
   * the replacement's schema becomes current, but the table keeps its history and must not drop the
   * concurrently added schema id "as though the concurrent commit never happened".
   */
  @TestTemplate
  public void testReplaceTransactionPreservesConcurrentlyAddedSchema() {
    table.updateProperties().set("random-snapshot-ids", "true").commit();

    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    int originalSchemaId = table.schema().schemaId();

    // Begin the replace before the concurrent schema change, so the replace's base is stale.
    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());
    replace.newAppend().appendFile(FILE_B).commit();

    // Concurrent writer evolves the schema: adds a column, producing a new current schema id.
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    table.refresh();
    int concurrentSchemaId = table.schema().schemaId();
    assertThat(concurrentSchemaId).isNotEqualTo(originalSchemaId);
    boolean concurrentSchemaHasExtra = table.schema().findField("extra") != null;
    assertThat(concurrentSchemaHasExtra).isTrue();

    replace.commitTransaction();
    table.refresh();

    // The concurrently added schema id must remain in the table's schema history after the replace.
    assertThat(table.schemas())
        .as("Concurrently added schema id must be preserved in history after replace")
        .containsKey(concurrentSchemaId);
  }

  /**
   * Verifies that rebasing the replacement onto refreshed metadata keeps the replacement schema's
   * field IDs stable. The staged data files were written against the stage-time replacement
   * schema's field IDs; re-deriving IDs by name against the concurrently-changed schema would shift
   * them and leave the committed schema no longer matching the staged data. Preserving the IDs
   * keeps the staged data readable.
   */
  @TestTemplate
  public void testReplaceTransactionPreservesFieldIdsUnderConcurrentSchemaChange() {
    table.updateProperties().set("random-snapshot-ids", "true").commit();

    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();

    // Replace with a schema that introduces a column NOT present in the original schema. Built
    // against the original (seq-1) base, "added_by_replace" receives a fresh field id.
    Schema replaceSchema =
        new Schema(
            required(10, "id", Types.IntegerType.get()),
            required(11, "data", Types.StringType.get()),
            required(12, "added_by_replace", Types.StringType.get()));
    Transaction replace = TestTables.beginReplace(tableDir, "test", replaceSchema, unpartitioned());
    replace.newAppend().appendFile(FILE_B).commit();

    int stagedReplaceColumnId =
        ((BaseTransaction) replace)
            .currentMetadata()
            .schema()
            .findField("added_by_replace")
            .fieldId();

    // Concurrent writer adds its own column to the original schema, consuming the next field id.
    table.updateSchema().addColumn("added_by_concurrent", Types.StringType.get()).commit();
    table.refresh();

    replace.commitTransaction();
    table.refresh();

    int committedReplaceColumnId = table.schema().findField("added_by_replace").fieldId();

    // The replacement column keeps the field id the staged data files were written against, even
    // though a concurrent schema change consumed that id in the refreshed base's schema.
    assertThat(committedReplaceColumnId)
        .as("Replacement column field id must be stable across rebase")
        .isEqualTo(stagedReplaceColumnId);
  }

  /**
   * Exercises a replace whose table is concurrently dropped after the transaction is staged. The
   * refreshed base is null (TestTables.refresh returns null for a missing table), so there is
   * nothing to rebase onto; the replace must commit the staged replacement as a create rather than
   * attempting to rebuild on a null base.
   */
  @TestTemplate
  public void testReplaceTransactionConcurrentlyDroppedTable() {
    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    assertThat(version()).isEqualTo(1);

    // Begin the replace against the existing table; base is captured non-null.
    Transaction replace = TestTables.beginReplace(tableDir, "test", table.schema(), table.spec());
    replace.newAppend().appendFile(FILE_B).commit();

    // Concurrent drop: the underlying table disappears before the replace commits, so refresh()
    // returns a null base.
    TestTables.clearTables();

    replace.commitTransaction();

    TableMetadata after = TestTables.readMetadata("test");
    assertThat(after).isNotNull();
    validateSnapshot(null, after.currentSnapshot(), FILE_B);
  }

  @TestTemplate
  public void testReplaceToCreateAndAppend() throws IOException {
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
