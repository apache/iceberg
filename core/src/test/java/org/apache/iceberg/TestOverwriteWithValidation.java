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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestOverwriteWithValidation extends TableTestBase {

  private static final String TABLE_NAME = "overwrite_table";

  private static final Schema DATE_SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC = PartitionSpec
      .builderFor(DATE_SCHEMA)
      .identity("date")
      .build();

  private static DataFile fileDay1;

  private static DataFile fileDay2;

  private static DataFile fileDay2Modified;

  private static DataFile fileDay2AnotherRange;

  private static final Expression EXPRESSION_DAY_2 = equal("date", "2018-06-09");

  private static final Expression EXPRESSION_DAY_2_ID_RANGE = and(
      greaterThanOrEqual("id", 5L),
      lessThanOrEqual("id", 9L));

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }

  private Table table = null;

  @Before
  public void before() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());
    this.table = TestTables.create(tableDir, TABLE_NAME, DATE_SCHEMA, PARTITION_SPEC);

    fileDay1 = DataFiles
            .builder(PARTITION_SPEC, table.location())
            .withPath("/path/to/data-1.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("date=2018-06-08")
            .withMetrics(new Metrics(5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 3L), // value count
                    ImmutableMap.of(1, 0L, 2, 2L), // null count
                    ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
            ))
            .build();

    fileDay2 = DataFiles
            .builder(PARTITION_SPEC, table.location())
            .withPath("/path/to/data-2.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("date=2018-06-09")
            .withMetrics(new Metrics(5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 3L), // value count
                    ImmutableMap.of(1, 0L, 2, 2L), // null count
                    ImmutableMap.of(1, longToBuffer(5L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(9L)) // upper bounds
            ))
            .build();

    fileDay2Modified = DataFiles
            .builder(PARTITION_SPEC, table.location())
            .withPath("/path/to/data-3.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("date=2018-06-09")
            .withMetrics(new Metrics(5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 3L), // value count
                    ImmutableMap.of(1, 0L, 2, 2L), // null count
                    ImmutableMap.of(1, longToBuffer(5L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(9L)) // upper bounds
            ))
            .build();

    fileDay2AnotherRange = DataFiles
            .builder(PARTITION_SPEC, table.location())
            .withPath("/path/to/data-3.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("date=2018-06-09")
            .withMetrics(new Metrics(5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 3L), // value count
                    ImmutableMap.of(1, 0L, 2, 2L), // null count
                    ImmutableMap.of(1, longToBuffer(10L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(14L)) // upper bounds
            ))
            .build();
  }

  @Test
  public void testOverwriteEmptyTableNotValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newOverwrite()
        .addFile(fileDay2Modified)
        .commit();

    validateTableFiles(table, fileDay2Modified);
  }

  @Test
  public void testOverwriteEmptyTableStrictValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(null, alwaysTrue())
        .commit();

    validateTableFiles(table, fileDay2Modified);
  }

  @Test
  public void testOverwriteEmptyTableValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2)
        .commit();

    validateTableFiles(table, fileDay2Modified);
  }

  @Test
  public void testOverwriteTableNotValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteTableStrictValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), alwaysTrue())
        .commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteTableValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2)
        .commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteCompatibleAdditionNotValidated() {
    table.newAppend()
        .appendFile(fileDay2)
        .commit();

    validateSnapshot(null, table.currentSnapshot(), table.location(), fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified);

    table.newAppend()
        .appendFile(fileDay1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteCompatibleAdditionStrictValidated() {
    table.newAppend()
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), alwaysTrue());

    table.newAppend()
        .appendFile(fileDay1)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "A file was appended",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteCompatibleAdditionValidated() {
    table.newAppend()
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteCompatibleDeletionValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newDelete()
        .deleteFile(fileDay1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay2Modified);
  }

  @Test
  public void testOverwriteIncompatibleAdditionValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1);

    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay2)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "A file was appended",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteIncompatibleDeletionValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newDelete()
        .deleteFile(fileDay2)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Missing required files to delete:",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteIncompatibleRewriteValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(fileDay2), ImmutableSet.of(fileDay2))
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "A file was appended",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteCompatibleExpirationAdditionValidated() {
    table.newAppend()
        .appendFile(fileDay2)
        .commit(); // id 1

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay1)
        .commit(); // id 2

    table.expireSnapshots()
        .expireSnapshotId(1L)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteCompatibleExpirationDeletionValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .appendFile(fileDay2)
        .commit(); // id 1

    Snapshot baseSnapshot = table.currentSnapshot();
    validateSnapshot(null, baseSnapshot, table.location(), fileDay1, fileDay2);

    OverwriteFiles overwrite = table.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newDelete()
        .deleteFile(fileDay1)
        .commit(); // id 2

    table.expireSnapshots()
        .expireSnapshotId(1L)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay2Modified);
  }

  @Test
  public void testOverwriteIncompatibleExpirationValidated() {
    table.newAppend()
        .appendFile(fileDay1)
        .commit(); // id 1

    Snapshot baseSnapshot = table.currentSnapshot();
    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay2)
        .commit(); // id 2

    table.newDelete()
        .deleteFile(fileDay1)
        .commit(); // id 3

    table.expireSnapshots()
        .expireSnapshotId(2L)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Cannot determine history",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteIncompatibleBaseExpirationEmptyTableValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay2)
        .commit(); // id 1

    table.newDelete()
        .deleteFile(fileDay1)
        .commit(); // id 2

    table.expireSnapshots()
        .expireSnapshotId(1L)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Cannot determine history",
        overwrite::commit);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }

  @Test
  public void testOverwriteAnotherRangeValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2_ID_RANGE);

    table.newAppend()
        .appendFile(fileDay1)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testOverwriteAnotherRangeWithinPartitionValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    Expression conflictDetectionFilter = and(EXPRESSION_DAY_2, EXPRESSION_DAY_2_ID_RANGE);
    OverwriteFiles overwrite = table.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(null, conflictDetectionFilter);

    table.newAppend()
        .appendFile(fileDay2AnotherRange)
        .commit();

    overwrite.commit();

    validateTableFiles(table, fileDay2AnotherRange, fileDay2Modified);
  }

  @Test
  public void testTransactionCompatibleAdditionValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    table.newAppend()
        .appendFile(fileDay2)
        .commit();

    Snapshot baseSnapshot = table.currentSnapshot();

    Transaction txn = table.newTransaction();
    OverwriteFiles overwrite = txn.newOverwrite()
        .deleteFile(fileDay2)
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(baseSnapshot.snapshotId(), EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay1)
        .commit();

    overwrite.commit();
    txn.commitTransaction();

    validateTableFiles(table, fileDay1, fileDay2Modified);
  }

  @Test
  public void testTransactionIncompatibleAdditionValidated() {
    Assert.assertNull("Should be empty table", table.currentSnapshot());

    Transaction txn = table.newTransaction();

    txn.newAppend()
        .appendFile(fileDay1)
        .commit();

    OverwriteFiles overwrite = txn.newOverwrite()
        .addFile(fileDay2Modified)
        .validateNoConflictingAppends(null, EXPRESSION_DAY_2);

    table.newAppend()
        .appendFile(fileDay2)
        .commit();
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    overwrite.commit();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "A file was appended",
        txn::commitTransaction);

    Assert.assertEquals("Should not create a new snapshot",
        committedSnapshotId, table.currentSnapshot().snapshotId());
  }
}
