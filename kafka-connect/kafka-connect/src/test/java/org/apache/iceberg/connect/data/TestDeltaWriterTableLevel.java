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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Table-level integration tests for the CDC delta writer.
 *
 * <p>Unlike the unit tests in {@link TestUnpartitionedDeltaWriter} and {@link
 * TestPartitionedDeltaWriter} which use mocked tables and only verify WriteResult metadata (file
 * counts, content types), these tests write to real filesystem-backed Iceberg tables, commit
 * transactions, and read back actual row data. This verifies end-to-end correctness: that equality
 * deletes actually mask prior-batch rows when the table is scanned.
 *
 * <p>Follows the pattern established by Flink's {@code TestDeltaTaskWriter}.
 *
 * <p>The schema uses a separate {@code payload} column (non-key, non-partition) to distinguish row
 * versions across batches. The {@code partition} column serves as the partition key in partitioned
 * mode and must remain stable for the same key across operations — Kafka Connect CDC only has the
 * new row (no before-image), so equality deletes are scoped to the row's partition and cannot reach
 * rows in a different partition.
 *
 * <p>See also {@link TestRecoveryScenario} for tests that exercise combined-commit recovery
 * scenarios and document the Iceberg V2 same-sequence limitation.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestDeltaWriterTableLevel extends TableLevelTestBase {

  // Identifier fields: (id, id2). Partition column: partition. Non-key data: payload.
  // Operation type: _op.
  static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "id2", Types.LongType.get()),
              Types.NestedField.required(3, "partition", Types.StringType.get()),
              Types.NestedField.required(4, "payload", Types.StringType.get()),
              Types.NestedField.required(5, "_op", Types.StringType.get())),
          ImmutableSet.of(1, 2));

  // Logical data columns for assertions (excludes _op which is operation metadata)
  static final String[] DATA_COLUMNS = {"id", "id2", "partition", "payload"};

  @Override
  protected Schema schema() {
    return SCHEMA;
  }

  @Override
  protected IcebergSinkConfig sinkConfig() {
    TableSinkConfig tableSinkConfig = mock(TableSinkConfig.class);
    when(tableSinkConfig.idColumns()).thenReturn(ImmutableList.of("id", "id2"));
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(tableSinkConfig);
    when(config.writeProps())
        .thenReturn(ImmutableMap.of("write.format.default", format.toString()));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesCdcField()).thenReturn("_op");
    return config;
  }

  /** Creates a record with all fields. */
  protected Record record(long id, long id2, String partition, String op, String payload) {
    Record rec = GenericRecord.create(SCHEMA);
    rec.setField("id", id);
    rec.setField("id2", id2);
    rec.setField("partition", partition);
    rec.setField("payload", payload);
    rec.setField("_op", op);
    return rec;
  }

  /**
   * Creates a record projected to the data columns for use in expected row sets. The _op field is
   * excluded because it's operation metadata, not part of the logical row.
   */
  protected Record expected(long id, long id2, String partition, String payload) {
    Schema projected = table.schema().select(DATA_COLUMNS);
    Record rec = GenericRecord.create(projected);
    rec.setField("id", id);
    rec.setField("id2", id2);
    rec.setField("partition", partition);
    rec.setField("payload", payload);
    return rec;
  }

  protected StructLikeSet expectedRowSet(Record... records) {
    StructLikeSet set = StructLikeSet.create(table.schema().select(DATA_COLUMNS).asStruct());
    for (Record rec : records) {
      set.add(rec);
    }
    return set;
  }

  /**
   * Cross-batch INSERT idempotency: a second INSERT for the same key in a new batch must supersede
   * the first batch's row. This is the core test for the deleteKey-before-INSERT fix — without it,
   * both rows survive (the duplicate bug).
   */
  @TestTemplate
  public void testCrossBatchInsertIdempotency() throws IOException {
    createAndInitTable();

    // Batch 1: INSERT key=(1,1)
    WriteResult batch1 = writeBatch(ImmutableList.of(record(1L, 1L, "part-a", "C", "v1")));
    commitTransaction(batch1);

    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(expectedRowSet(expected(1L, 1L, "part-a", "v1")));

    // Batch 2 (new writer): INSERT key=(1,1) again with different payload
    WriteResult batch2 = writeBatch(ImmutableList.of(record(1L, 1L, "part-a", "C", "v2")));
    commitTransaction(batch2);

    // Only the batch 2 row should survive — the equality delete from batch 2's
    // INSERT must mask batch 1's row
    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(expectedRowSet(expected(1L, 1L, "part-a", "v2")));
  }

  /**
   * Cross-batch UPDATE after INSERT: an UPDATE in a new batch must replace the prior batch's row.
   */
  @TestTemplate
  public void testCrossBatchUpdateAfterInsert() throws IOException {
    createAndInitTable();

    // Batch 1: INSERT key=(1,1)
    WriteResult batch1 = writeBatch(ImmutableList.of(record(1L, 1L, "part-a", "C", "original")));
    commitTransaction(batch1);

    // Batch 2: UPDATE key=(1,1) (same partition, different payload)
    WriteResult batch2 = writeBatch(ImmutableList.of(record(1L, 1L, "part-a", "U", "updated")));
    commitTransaction(batch2);

    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(expectedRowSet(expected(1L, 1L, "part-a", "updated")));
  }

  /**
   * Mixed CDC operations in a single batch. Verifies that within-batch position deletes and
   * equality deletes combine correctly.
   */
  @TestTemplate
  public void testCdcMixedOperationsEndToEnd() throws IOException {
    createAndInitTable();

    // All operations for the same key stay in the same partition
    WriteResult result =
        writeBatch(
            ImmutableList.of(
                record(1L, 1L, "part-a", "C", "a-orig"),
                record(2L, 2L, "part-b", "C", "b-orig"),
                record(3L, 3L, "part-c", "C", "c-orig"),
                record(1L, 1L, "part-a", "U", "a-updated"),
                record(2L, 2L, "part-b", "D", "b-orig"),
                record(4L, 4L, "part-d", "C", "d-orig")));
    commitTransaction(result);

    // key=(1,1) updated, key=(2,2) deleted, key=(3,3) and key=(4,4) inserted
    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(
            expectedRowSet(
                expected(1L, 1L, "part-a", "a-updated"),
                expected(3L, 3L, "part-c", "c-orig"),
                expected(4L, 4L, "part-d", "d-orig")));
  }

  /** Cross-batch with multiple keys: DELETE, UPDATE, and INSERT across two batches. */
  @TestTemplate
  public void testCrossBatchMultipleKeys() throws IOException {
    createAndInitTable();

    // Batch 1: INSERT key=(1,1) and key=(2,2)
    WriteResult batch1 =
        writeBatch(
            ImmutableList.of(
                record(1L, 1L, "part-a", "C", "a-v1"), record(2L, 2L, "part-b", "C", "b-v1")));
    commitTransaction(batch1);

    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(
            expectedRowSet(expected(1L, 1L, "part-a", "a-v1"), expected(2L, 2L, "part-b", "b-v1")));

    // Batch 2: DELETE key=(1,1), UPDATE key=(2,2) (same partition), INSERT key=(3,3)
    WriteResult batch2 =
        writeBatch(
            ImmutableList.of(
                record(1L, 1L, "part-a", "D", "a-v1"),
                record(2L, 2L, "part-b", "U", "b-v2"),
                record(3L, 3L, "part-c", "C", "c-v1")));
    commitTransaction(batch2);

    // key=(1,1) deleted, key=(2,2) updated, key=(3,3) inserted
    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(
            expectedRowSet(expected(2L, 2L, "part-b", "b-v2"), expected(3L, 3L, "part-c", "c-v1")));
  }

  /** Sanity check: distinct-key INSERTs should all survive with no data loss. */
  @TestTemplate
  public void testInsertOnlyNoDuplicates() throws IOException {
    createAndInitTable();

    WriteResult result =
        writeBatch(
            ImmutableList.of(
                record(1L, 1L, "part-a", "C", "a"),
                record(2L, 2L, "part-b", "C", "b"),
                record(3L, 3L, "part-c", "C", "c")));
    commitTransaction(result);

    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(
            expectedRowSet(
                expected(1L, 1L, "part-a", "a"),
                expected(2L, 2L, "part-b", "b"),
                expected(3L, 3L, "part-c", "c")));
  }

  /**
   * The shielding bug scenario: INSERT(key=1) followed by UPDATE(key=1) in the same batch, with a
   * prior batch's row for key=1 still in the table.
   *
   * <p>Without the fix, INSERT's write() populates insertedRowMap before UPDATE's deleteKey(),
   * causing UPDATE to take the position-delete path. No equality delete is emitted, so the prior
   * batch's row survives as a duplicate.
   *
   * <p>With the fix, INSERT calls deleteKey() first (while insertedRowMap is still empty),
   * producing the equality delete that masks the prior batch's row.
   */
  @TestTemplate
  public void testShieldingBugCrossBatch() throws IOException {
    createAndInitTable();

    // Batch 1: INSERT key=(1,1)
    WriteResult batch1 = writeBatch(ImmutableList.of(record(1L, 1L, "part-a", "C", "batch1")));
    commitTransaction(batch1);

    // Batch 2: INSERT key=(1,1) followed by UPDATE key=(1,1) in the same batch.
    // Both stay in the same partition to ensure equality deletes reach batch 1's row.
    WriteResult batch2 =
        writeBatch(
            ImmutableList.of(
                record(1L, 1L, "part-a", "C", "intermediate"),
                record(1L, 1L, "part-a", "U", "final")));
    commitTransaction(batch2);

    // Only the final updated row should survive
    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(expectedRowSet(expected(1L, 1L, "part-a", "final")));
  }

  /**
   * Composite key with distinct id and id2 values. Verifies that both key columns participate in
   * equality deletes — rows sharing the same {@code id} but different {@code id2} are distinct keys
   * and must not interfere with each other.
   */
  @TestTemplate
  public void testCompositeKeyDistinctValues() throws IOException {
    createAndInitTable();

    // Two rows sharing the same id but with different id2 — they are distinct keys
    WriteResult batch1 =
        writeBatch(
            ImmutableList.of(
                record(1L, 10L, "part-a", "C", "row-1-10"),
                record(1L, 20L, "part-a", "C", "row-1-20")));
    commitTransaction(batch1);

    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(
            expectedRowSet(
                expected(1L, 10L, "part-a", "row-1-10"), expected(1L, 20L, "part-a", "row-1-20")));

    // Update only key (1, 10) — key (1, 20) must survive unchanged
    WriteResult batch2 =
        writeBatch(ImmutableList.of(record(1L, 10L, "part-a", "U", "row-1-10-v2")));
    commitTransaction(batch2);

    assertThat(actualRowSet(DATA_COLUMNS))
        .isEqualTo(
            expectedRowSet(
                expected(1L, 10L, "part-a", "row-1-10-v2"),
                expected(1L, 20L, "part-a", "row-1-20")));
  }
}
