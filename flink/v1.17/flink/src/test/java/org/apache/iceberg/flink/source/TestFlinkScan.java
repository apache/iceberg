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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestFlinkScan {
  @RegisterExtension
  protected static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @TempDir protected Path temporaryDirectory;

  @RegisterExtension
  protected static final HadoopCatalogExtension catalogExtension =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  @Parameter protected FileFormat fileFormat;

  @Parameters(name = "format={0}")
  public static Collection<FileFormat> fileFormat() {
    return Arrays.asList(FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC);
  }

  protected TableLoader tableLoader() {
    return catalogExtension.tableLoader();
  }

  protected abstract List<Row> runWithProjection(String... projected) throws Exception;

  protected abstract List<Row> runWithFilter(
      Expression filter, String sqlFilter, boolean caseSensitive) throws Exception;

  protected List<Row> runWithFilter(Expression filter, String sqlFilter) throws Exception {
    return runWithFilter(filter, sqlFilter, true);
  }

  protected abstract List<Row> runWithOptions(Map<String, String> options) throws Exception;

  protected abstract List<Row> run() throws Exception;

  @TestTemplate
  public void testUnpartitionedTable() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, temporaryDirectory).appendToTable(expectedRecords);
    TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testPartitionedTable() throws Exception {
    Table table =
        catalogExtension
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    new GenericAppenderHelper(table, fileFormat, temporaryDirectory)
        .appendToTable(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testProjection() throws Exception {
    Table table =
        catalogExtension
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
    List<Record> inputRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    new GenericAppenderHelper(table, fileFormat, temporaryDirectory)
        .appendToTable(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), inputRecords);
    assertRows(runWithProjection("data"), Row.of(inputRecords.get(0).get(0)));
  }

  @TestTemplate
  public void testIdentityPartitionProjections() throws Exception {
    Schema logSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "dt", Types.StringType.get()),
            Types.NestedField.optional(3, "level", Types.StringType.get()),
            Types.NestedField.optional(4, "message", Types.StringType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(logSchema).identity("dt").identity("level").build();

    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, logSchema, spec);
    List<Record> inputRecords = RandomGenericData.generate(logSchema, 10, 0L);

    int idx = 0;
    AppendFiles append = table.newAppend();
    for (Record record : inputRecords) {
      record.set(1, "2020-03-2" + idx);
      record.set(2, Integer.toString(idx));
      append.appendFile(
          new GenericAppenderHelper(table, fileFormat, temporaryDirectory)
              .writeFile(
                  org.apache.iceberg.TestHelpers.Row.of("2020-03-2" + idx, Integer.toString(idx)),
                  ImmutableList.of(record)));
      idx += 1;
    }
    append.commit();

    // individual fields
    validateIdentityPartitionProjections(table, Collections.singletonList("dt"), inputRecords);
    validateIdentityPartitionProjections(table, Collections.singletonList("level"), inputRecords);
    validateIdentityPartitionProjections(table, Collections.singletonList("message"), inputRecords);
    validateIdentityPartitionProjections(table, Collections.singletonList("id"), inputRecords);
    // field pairs
    validateIdentityPartitionProjections(table, Arrays.asList("dt", "message"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("level", "message"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("dt", "level"), inputRecords);
    // out-of-order pairs
    validateIdentityPartitionProjections(table, Arrays.asList("message", "dt"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("message", "level"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("level", "dt"), inputRecords);
    // out-of-order triplets
    validateIdentityPartitionProjections(
        table, Arrays.asList("dt", "level", "message"), inputRecords);
    validateIdentityPartitionProjections(
        table, Arrays.asList("level", "dt", "message"), inputRecords);
    validateIdentityPartitionProjections(
        table, Arrays.asList("dt", "message", "level"), inputRecords);
    validateIdentityPartitionProjections(
        table, Arrays.asList("level", "message", "dt"), inputRecords);
    validateIdentityPartitionProjections(
        table, Arrays.asList("message", "dt", "level"), inputRecords);
    validateIdentityPartitionProjections(
        table, Arrays.asList("message", "level", "dt"), inputRecords);
  }

  private void validateIdentityPartitionProjections(
      Table table, List<String> projectedFields, List<Record> inputRecords) throws Exception {
    List<Row> rows = runWithProjection(projectedFields.toArray(new String[0]));

    for (int pos = 0; pos < inputRecords.size(); pos++) {
      Record inputRecord = inputRecords.get(pos);
      Row actualRecord = rows.get(pos);

      for (int i = 0; i < projectedFields.size(); i++) {
        String name = projectedFields.get(i);
        assertThat(inputRecord.getField(name))
            .as("Projected field " + name + " should match")
            .isEqualTo(actualRecord.getField(i));
      }
    }
  }

  @TestTemplate
  public void testSnapshotReads() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(expectedRecords);
    long snapshotId = table.currentSnapshot().snapshotId();

    long timestampMillis = table.currentSnapshot().timestampMillis();

    // produce another timestamp
    waitUntilAfter(timestampMillis);
    helper.appendToTable(RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L));

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("snapshot-id", Long.toString(snapshotId))),
        expectedRecords,
        TestFixtures.SCHEMA);
    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("as-of-timestamp", Long.toString(timestampMillis))),
        expectedRecords,
        TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testTagReads() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    List<Record> expectedRecords1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(expectedRecords1);
    long snapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createTag("t1", snapshotId).commit();

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("tag", "t1")), expectedRecords1, TestFixtures.SCHEMA);

    List<Record> expectedRecords2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(expectedRecords2);
    snapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().replaceTag("t1", snapshotId).commit();

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(expectedRecords1);
    expectedRecords.addAll(expectedRecords2);
    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("tag", "t1")), expectedRecords, TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testBranchReads() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    List<Record> expectedRecordsBase = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(expectedRecordsBase);
    long snapshotId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotId).commit();

    List<Record> expectedRecordsForBranch = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(branchName, expectedRecordsForBranch);

    List<Record> expectedRecordsForMain = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(expectedRecordsForMain);

    List<Record> branchExpectedRecords = Lists.newArrayList();
    branchExpectedRecords.addAll(expectedRecordsBase);
    branchExpectedRecords.addAll(expectedRecordsForBranch);

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("branch", branchName)),
        branchExpectedRecords,
        TestFixtures.SCHEMA);

    List<Record> mainExpectedRecords = Lists.newArrayList();
    mainExpectedRecords.addAll(expectedRecordsBase);
    mainExpectedRecords.addAll(expectedRecordsForMain);

    TestHelpers.assertRecords(run(), mainExpectedRecords, TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testIncrementalReadViaTag() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long snapshotId1 = table.currentSnapshot().snapshotId();
    String startTag = "t1";
    table.manageSnapshots().createTag(startTag, snapshotId1).commit();

    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1L);
    helper.appendToTable(records2);

    List<Record> records3 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 2L);
    helper.appendToTable(records3);
    long snapshotId3 = table.currentSnapshot().snapshotId();
    String endTag = "t2";
    table.manageSnapshots().createTag(endTag, snapshotId3).commit();

    helper.appendToTable(RandomGenericData.generate(TestFixtures.SCHEMA, 1, 3L));

    List<Record> expected = Lists.newArrayList();
    expected.addAll(records2);
    expected.addAll(records3);

    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-tag", startTag)
                .put("end-tag", endTag)
                .buildOrThrow()),
        expected,
        TestFixtures.SCHEMA);

    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-id", Long.toString(snapshotId1))
                .put("end-tag", endTag)
                .buildOrThrow()),
        expected,
        TestFixtures.SCHEMA);

    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-tag", startTag)
                .put("end-snapshot-id", Long.toString(snapshotId3))
                .buildOrThrow()),
        expected,
        TestFixtures.SCHEMA);

    assertThatThrownBy(
            () ->
                runWithOptions(
                    ImmutableMap.<String, String>builder()
                        .put("start-tag", startTag)
                        .put("end-tag", endTag)
                        .put("start-snapshot-id", Long.toString(snapshotId1))
                        .buildOrThrow()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");

    assertThatThrownBy(
            () ->
                runWithOptions(
                    ImmutableMap.<String, String>builder()
                        .put("start-tag", startTag)
                        .put("end-tag", endTag)
                        .put("end-snapshot-id", Long.toString(snapshotId3))
                        .buildOrThrow()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot specify more than one of end-snapshot-id, end-tag, or end-snapshot-timestamp.");
  }

  @TestTemplate
  public void testIncrementalRead() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long snapshotId1 = table.currentSnapshot().snapshotId();

    // snapshot 2
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1L);
    helper.appendToTable(records2);

    List<Record> records3 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 2L);
    helper.appendToTable(records3);
    long snapshotId3 = table.currentSnapshot().snapshotId();

    // snapshot 4
    helper.appendToTable(RandomGenericData.generate(TestFixtures.SCHEMA, 1, 3L));

    List<Record> expected2 = Lists.newArrayList();
    expected2.addAll(records2);
    expected2.addAll(records3);
    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-id", Long.toString(snapshotId1))
                .put("end-snapshot-id", Long.toString(snapshotId3))
                .buildOrThrow()),
        expected2,
        TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testIncrementalReadWithTimestampRange() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    // snapshot 1
    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long timestampMillis1 = table.currentSnapshot().timestampMillis();
    org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis1 + 2);

    // snapshot 2
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records2);
    long timestampMillis2 = table.currentSnapshot().timestampMillis();
    long timestampMillisAfter2 = org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis2);

    // snapshot 3
    List<Record> records3 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1L);
    helper.appendToTable(records3);

    // snapshot 4
    List<Record> records4 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 2L);
    helper.appendToTable(records4);
    long timestampMillis4 = table.currentSnapshot().timestampMillis();

    List<Record> expected = Lists.newArrayList();
    expected.addAll(records3);
    expected.addAll(records4);
    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-timestamp", Long.toString(timestampMillis2))
                .put("end-snapshot-timestamp", Long.toString(timestampMillis4))
                .buildOrThrow()),
        expected,
        TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testIncrementalReadWithTimestampRangeAfter() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    // snapshot 1
    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long timestampMillis1 = table.currentSnapshot().timestampMillis();
    org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis1 + 2);

    // snapshot 2
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records2);
    long timestampMillis2 = table.currentSnapshot().timestampMillis();
    long timestampMillisAfter2 = org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis2);

    // snapshot 3
    List<Record> records3 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1L);
    helper.appendToTable(records3);

    // snapshot 4
    List<Record> records4 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 2L);
    helper.appendToTable(records4);
    long timestampMillis4 = table.currentSnapshot().timestampMillis();
    long timestampMillisAfter4 = org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis4);

    // snapshot 5
    helper.appendToTable(RandomGenericData.generate(TestFixtures.SCHEMA, 1, 3L));
    long timestampMillisAfter5 =
        org.apache.iceberg.TestHelpers.waitUntilAfter(table.currentSnapshot().timestampMillis());

    List<Record> expected = Lists.newArrayList();
    expected.addAll(records3);
    expected.addAll(records4);

    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-timestamp", Long.toString(timestampMillisAfter2))
                .put("end-snapshot-timestamp", Long.toString(timestampMillis4))
                .buildOrThrow()),
        expected,
        TestFixtures.SCHEMA);

    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-timestamp", Long.toString(timestampMillis2))
                .put("end-snapshot-timestamp", Long.toString(timestampMillisAfter4))
                .buildOrThrow()),
        expected,
        TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testIncrementalReadWithTimestampRangeBefore() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    // snapshot 1
    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long timestampMillis1 = table.currentSnapshot().timestampMillis();
    org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis1 + 2);

    // snapshot 2
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records2);
    long timestampMillis2 = table.currentSnapshot().timestampMillis();
    org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis2);

    // snapshot 3
    List<Record> records3 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1L);
    helper.appendToTable(records3);

    // snapshot 4
    List<Record> records4 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 2L);
    helper.appendToTable(records4);
    long timestampMillis4 = table.currentSnapshot().timestampMillis();

    List<Record> expected2 = Lists.newArrayList();
    expected2.addAll(records2);
    expected2.addAll(records3);
    expected2.addAll(records4);
    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-timestamp", Long.toString(timestampMillis2 - 1))
                .put("end-snapshot-timestamp", Long.toString(timestampMillis4))
                .buildOrThrow()),
        expected2,
        TestFixtures.SCHEMA);

    TestHelpers.assertRecords(
        runWithOptions(
            ImmutableMap.<String, String>builder()
                .put("start-snapshot-timestamp", Long.toString(timestampMillis2))
                .put("end-snapshot-timestamp", Long.toString(timestampMillis4 - 1))
                .buildOrThrow()),
        records3,
        TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testIncrementalReadWithInvalidParameterShouldFail() throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);

    // snapshot 1
    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long timestampMillis1 = table.currentSnapshot().timestampMillis();
    org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis1 + 2);

    // snapshot 2
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    helper.appendToTable(records2);
    long timestampMillis2 = table.currentSnapshot().timestampMillis();
    long timestampMillisAfter2 = org.apache.iceberg.TestHelpers.waitUntilAfter(timestampMillis2);

    Assertions.assertThatThrownBy(
            () ->
                runWithOptions(
                    ImmutableMap.<String, String>builder()
                        .put("start-snapshot-timestamp", Long.toString(timestampMillis1))
                        .put("end-snapshot-timestamp", Long.toString(timestampMillis2))
                        .put(
                            "start-snapshot-id",
                            Long.toString(table.currentSnapshot().snapshotId()))
                        .buildOrThrow()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");

    Assertions.assertThatThrownBy(
            () ->
                runWithOptions(
                    ImmutableMap.<String, String>builder()
                        .put("start-snapshot-timestamp", Long.toString(timestampMillis1))
                        .put("end-snapshot-timestamp", Long.toString(timestampMillis2))
                        .put("start-tag", "tag1")
                        .buildOrThrow()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");

    Assertions.assertThatThrownBy(
            () ->
                runWithOptions(
                    ImmutableMap.<String, String>builder()
                        .put("start-snapshot-timestamp", Long.toString(timestampMillis2))
                        .put("end-snapshot-timestamp", Long.toString(timestampMillis1))
                        .buildOrThrow()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a parent ancestor of end snapshot");

    Assertions.assertThatThrownBy(
            () ->
                runWithOptions(
                    ImmutableMap.<String, String>builder()
                        .put("start-snapshot-timestamp", Long.toString(timestampMillisAfter2))
                        .buildOrThrow()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find a snapshot older than");
  }

  @TestTemplate
  public void testFilterExpPartition() throws Exception {
    Table table =
        catalogExtension
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);

    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    expectedRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);
    DataFile dataFile1 =
        helper.writeFile(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    DataFile dataFile2 =
        helper.writeFile(
            org.apache.iceberg.TestHelpers.Row.of("2020-03-21", 0),
            RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);
    TestHelpers.assertRecords(
        runWithFilter(Expressions.equal("dt", "2020-03-20"), "where dt='2020-03-20'", true),
        expectedRecords,
        TestFixtures.SCHEMA);
  }

  private void testFilterExp(Expression filter, String sqlFilter, boolean caseSensitive)
      throws Exception {
    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 3, 0L);
    expectedRecords.get(0).set(0, "a");
    expectedRecords.get(1).set(0, "b");
    expectedRecords.get(2).set(0, "c");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);
    DataFile dataFile = helper.writeFile(expectedRecords);
    helper.appendToTable(dataFile);

    List<Row> actual =
        runWithFilter(Expressions.greaterThanOrEqual("data", "b"), "where data>='b'", true);

    TestHelpers.assertRecords(actual, expectedRecords.subList(1, 3), TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testFilterExp() throws Exception {
    testFilterExp(Expressions.greaterThanOrEqual("data", "b"), "where data>='b'", true);
  }

  @TestTemplate
  public void testFilterExpCaseInsensitive() throws Exception {
    // sqlFilter does not support case-insensitive filtering:
    // https://issues.apache.org/jira/browse/FLINK-16175
    testFilterExp(Expressions.greaterThanOrEqual("DATA", "b"), "where data>='b'", false);
  }

  @TestTemplate
  public void testPartitionTypes() throws Exception {
    Schema typesSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "decimal", Types.DecimalType.of(38, 18)),
            Types.NestedField.optional(3, "str", Types.StringType.get()),
            Types.NestedField.optional(4, "binary", Types.BinaryType.get()),
            Types.NestedField.optional(5, "date", Types.DateType.get()),
            Types.NestedField.optional(6, "time", Types.TimeType.get()),
            Types.NestedField.optional(7, "timestamp", Types.TimestampType.withoutZone()));
    PartitionSpec spec =
        PartitionSpec.builderFor(typesSchema)
            .identity("decimal")
            .identity("str")
            .identity("binary")
            .identity("date")
            .identity("time")
            .identity("timestamp")
            .build();

    Table table =
        catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, typesSchema, spec);
    List<Record> records = RandomGenericData.generate(typesSchema, 10, 0L);
    GenericAppenderHelper appender =
        new GenericAppenderHelper(table, fileFormat, temporaryDirectory);
    for (Record record : records) {
      org.apache.iceberg.TestHelpers.Row partition =
          org.apache.iceberg.TestHelpers.Row.of(
              record.get(1),
              record.get(2),
              record.get(3),
              record.get(4) == null ? null : DateTimeUtil.daysFromDate((LocalDate) record.get(4)),
              record.get(5) == null ? null : DateTimeUtil.microsFromTime((LocalTime) record.get(5)),
              record.get(6) == null
                  ? null
                  : DateTimeUtil.microsFromTimestamp((LocalDateTime) record.get(6)));
      appender.appendToTable(partition, Collections.singletonList(record));
    }

    TestHelpers.assertRecords(run(), records, typesSchema);
  }

  @TestTemplate
  public void testCustomizedFlinkDataTypes() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.StringType.get())),
            Types.NestedField.required(
                4, "arr", Types.ListType.ofRequired(5, Types.StringType.get())));
    Table table = catalogExtension.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, schema);
    List<Record> records = RandomGenericData.generate(schema, 10, 0L);
    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, temporaryDirectory);
    helper.appendToTable(records);
    TestHelpers.assertRecords(run(), records, schema);
  }

  private static void assertRows(List<Row> results, Row... expected) {
    TestHelpers.assertRows(results, Arrays.asList(expected));
  }

  private static void waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
  }
}
