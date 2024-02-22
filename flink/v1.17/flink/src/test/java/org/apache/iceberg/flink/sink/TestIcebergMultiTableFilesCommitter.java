/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.TableAwareWriteResult;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ThreadPools;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.IcebergFilesCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class TestIcebergMultiTableFilesCommitter {

    private CatalogLoader catalogLoader;
    private TableLoader tableLoader;
    private MockedStatic<TableLoader> mockStatic = Mockito.mockStatic(TableLoader.class);

    private TestTables.TestTable table1;
    private TestTables.TestTable table2;

    @Rule
    public TemporaryFolder temp1 = new TemporaryFolder();

    @Rule
    public TemporaryFolder temp2 = new TemporaryFolder();

    private static final Configuration CONF = new Configuration();

    private File flinkManifestFolder1;
    private File flinkManifestFolder2;
    protected File tableDir1 = null;
    protected File metadataDir1 = null;
    protected File tableDir2 = null;
    protected File metadataDir2 = null;

    private final FileFormat format;
    private final String branch;;
    private final int formatVersion;

    public TestIcebergMultiTableFilesCommitter(String format, int formatVersion, String branch) {
        this.format = FileFormat.fromString(format);
        this.branch = branch;
        this.formatVersion = formatVersion;
    }

    @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion = {1}, branch = {2}")
    public static Object[][] parameters() {
        return new Object[][] {
                new Object[] {"avro", 1, "main"},
                new Object[] {"avro", 2, "test-branch"},
                new Object[] {"parquet", 1, "main"},
                new Object[] {"parquet", 2, "test-branch"},
                new Object[] {"orc", 1, "main"},
                new Object[] {"orc", 2, "test-branch"}
        };
    }

    @After
    public void after() {
        mockStatic.close();
    }

    @Before
    public void setupTable() throws IOException {
        flinkManifestFolder1 = temp1.newFolder();

        tableDir1 = temp1.newFolder();
        metadataDir1 = new File(tableDir1, "metadata");
        Assert.assertTrue(tableDir1.delete());

        // Construct the iceberg table.
        table1 = create(tableDir1, "table1", SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());
        table1
                .updateProperties()
                .set(DEFAULT_FILE_FORMAT, format.name())
                .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder1.getAbsolutePath())
                .set(MAX_CONTINUOUS_EMPTY_COMMITS, "1")
                .commit();

        flinkManifestFolder2 = temp2.newFolder();
        tableDir2 = temp2.newFolder();
        metadataDir2 = new File(tableDir2, "metadata");
        Assert.assertTrue(tableDir2.delete());
        table2 = create(tableDir2, "table2", SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());
        table2
                .updateProperties()
                .set(DEFAULT_FILE_FORMAT, format.name())
                .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder2.getAbsolutePath())
                .set(MAX_CONTINUOUS_EMPTY_COMMITS, "1")
                .commit();
        catalogLoader = Mockito.mock(CatalogLoader.class);
        tableLoader = Mockito.mock(TableLoader.class);
        mockStatic
                .when(() -> TableLoader.fromCatalog(Mockito.any(), Mockito.any()))
                .thenReturn(tableLoader);
    }

    @After
    public void cleanupTables() {
        TestTables.clearTables();
    }

    @Test
    public void testCommitTxnWithoutDataFiles() throws Exception {
        long checkpointId = 0;
        long timestamp = 0;
        JobID jobId = new JobID();
        OperatorID operatorId;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            Mockito.when(tableLoader.loadTable()).thenReturn(table1).thenReturn(table2);
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            SimpleDataUtil.assertTableRows(table1, Lists.newArrayList(), branch);
            SimpleDataUtil.assertTableRows(table2, Lists.newArrayList(), branch);
            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // It's better to advance the max-committed-checkpoint-id in iceberg snapshot, so that the
            // future flink job
            // failover won't fail.
            for (int i = 1; i <= 3; i++) {
                harness.snapshot(++checkpointId, ++timestamp);
                assertFlinkManifests(0, flinkManifestFolder1);
                assertFlinkManifests(0, flinkManifestFolder2);

                harness.notifyOfCompletedCheckpoint(checkpointId);
                assertFlinkManifests(0, flinkManifestFolder1);
                assertFlinkManifests(0, flinkManifestFolder2);

                // This is 0 because table will be identified only after process element. Till then, no snapshot commit will happen
                assertSnapshotSize(table1, 0);
                assertSnapshotSize(table2, 0);
                // This is -1 because checkpoint will not move ahead since table disovery happens only during process elements
                assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1);
                assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1);
            }
        }
    }

    private TableAwareWriteResult of(DataFile dataFile, String tablename) {
        return new TableAwareWriteResult(WriteResult.builder().addDataFiles(dataFile).build(),  TableIdentifier.of("dummy", tablename));
    }

    private TableAwareWriteResult of(DataFile dataFile, DeleteFile deleteFile, String tablename) {
        return new TableAwareWriteResult(WriteResult.builder().addDataFiles(dataFile).addDeleteFiles(deleteFile).build(), TableIdentifier.of("dummy", tablename));
    }

    @Test
    public void testWriteToOneTable() throws Exception {
        // Test with 3 continues checkpoints:
        //   1. snapshotState for checkpoint#1
        //   2. notifyCheckpointComplete for checkpoint#1
        //   3. snapshotState for checkpoint#2
        //   4. notifyCheckpointComplete for checkpoint#2
        //   5. snapshotState for checkpoint#3
        //   6. notifyCheckpointComplete for checkpoint#3
        long timestamp = 0;

        JobID jobID = new JobID();
        OperatorID operatorId;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobID)) {
            Mockito.when(tableLoader.loadTable()).thenReturn(table1);
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 0);

            List<RowData> rows = Lists.newArrayListWithExpectedSize(3);
            for (int i = 1; i <= 3; i++) {
                RowData rowData = SimpleDataUtil.createRowData(i, "hello" + i);
                DataFile dataFile = writeDataFile(table1, "data-" + i, ImmutableList.of(rowData));
                harness.processElement(of(dataFile, table1.name()), ++timestamp);
                rows.add(rowData);

                harness.snapshot(i, ++timestamp);
                assertFlinkManifests(1, flinkManifestFolder1);

                harness.notifyOfCompletedCheckpoint(i);
                assertFlinkManifests(0, flinkManifestFolder1);

                SimpleDataUtil.assertTableRows(table1, ImmutableList.copyOf(rows), branch);
                assertSnapshotSize(table1, i);
                assertMaxCommittedCheckpointId(table1, jobID, operatorId, i);
                Assert.assertEquals(
                        TestIcebergMultiTableFilesCommitter.class.getName(),
                        SimpleDataUtil.latestSnapshot(table1, branch).summary().get("flink.test"));
            }
        }
    }

    @Test
    public void testWrite2Tables() throws Exception {
        // Test with 3 continues checkpoints:
        //   1. snapshotState for checkpoint#1
        //   2. notifyCheckpointComplete for checkpoint#1
        //   3. snapshotState for checkpoint#2
        //   4. notifyCheckpointComplete for checkpoint#2
        //   5. snapshotState for checkpoint#3
        //   6. notifyCheckpointComplete for checkpoint#3
        long timestamp = 0;
        JobID jobID = new JobID();
        OperatorID operatorId;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobID)) {
            createAlternateTableMock();
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);

            List<RowData> rows = Lists.newArrayListWithExpectedSize(3);
            for (int i = 1; i <= 3; i++) {
                RowData rowData = SimpleDataUtil.createRowData(i, "hello" + i);
                DataFile dataFile1 = writeDataFile(table1, "data-table1" + i, ImmutableList.of(rowData));
                DataFile dataFile2 = writeDataFile(table2, "data-table2" + i, ImmutableList.of(rowData));
                harness.processElement(of(dataFile1, table1.name()), ++timestamp);
                harness.processElement(of(dataFile2, table2.name()),timestamp);
                rows.add(rowData);

                harness.snapshot(i, ++timestamp);
                assertFlinkManifests(1, flinkManifestFolder1);
                assertFlinkManifests(1, flinkManifestFolder2);

                harness.notifyOfCompletedCheckpoint(i);
                assertFlinkManifests(0, flinkManifestFolder1);
                assertFlinkManifests(0, flinkManifestFolder2);

                SimpleDataUtil.assertTableRows(table1, ImmutableList.copyOf(rows), branch);
                SimpleDataUtil.assertTableRows(table2, ImmutableList.copyOf(rows), branch);
                assertSnapshotSize(table1, i);
                assertSnapshotSize(table2, i);
                assertMaxCommittedCheckpointId(table1, jobID, operatorId, i);
                assertMaxCommittedCheckpointId(table2, jobID, operatorId, i);
                Assert.assertEquals(
                        TestIcebergMultiTableFilesCommitter.class.getName(),
                        SimpleDataUtil.latestSnapshot(table1, branch).summary().get("flink.test"));
                Assert.assertEquals(
                        TestIcebergMultiTableFilesCommitter.class.getName(),
                        SimpleDataUtil.latestSnapshot(table2, branch).summary().get("flink.test"));
            }
        }
    }

    @Test
    public void testMaxContinuousEmptyCommits() throws Exception {
        table1.updateProperties().set(MAX_CONTINUOUS_EMPTY_COMMITS, "3").commit();
        table2.updateProperties().set(MAX_CONTINUOUS_EMPTY_COMMITS, "2").commit();

        JobID jobId = new JobID();
        long checkpointId = 0;
        long timestamp = 0;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            createAlternateTableMock();
            harness.setup();
            harness.open();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);

            for (int i = 1; i <= 9; i++) {
                harness.snapshot(++checkpointId, ++timestamp);
                harness.notifyOfCompletedCheckpoint(checkpointId);
                assertSnapshotSize(table1, 0);
                assertSnapshotSize(table2, 0);
            }
            RowData rowData = SimpleDataUtil.createRowData(0, "hello" + 0);
            DataFile dataFile1 = writeDataFile(table1, "data-table1" + 0, ImmutableList.of(rowData));
            DataFile dataFile2 = writeDataFile(table2, "data-table2" + 0, ImmutableList.of(rowData));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()),timestamp);
            harness.snapshot(++checkpointId, ++timestamp);
            harness.notifyOfCompletedCheckpoint(checkpointId);
            for (int i = 1; i <= 9; i++) {
                harness.snapshot(++checkpointId, ++timestamp);
                harness.notifyOfCompletedCheckpoint(checkpointId);
                assertSnapshotSize(table1, 1 + (i/3));
                assertSnapshotSize(table2, 1 + (i/2));
            }
        }
    }

    @Test
    public void testOrderedEventsBetweenCheckpoints() throws Exception {
        // It's possible that two checkpoints happen in the following orders:
        //   1. snapshotState for checkpoint#1;
        //   2. snapshotState for checkpoint#2;
        //   3. notifyCheckpointComplete for checkpoint#1;
        //   4. notifyCheckpointComplete for checkpoint#2;
        long timestamp = 0;

        JobID jobId = new JobID();
        OperatorID operatorId;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            createAlternateTableMock();
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData row1 = SimpleDataUtil.createRowData(1, "hello");
            DataFile dataFile1 = writeDataFile(table1, "data-1", ImmutableList.of(row1));
            DataFile dataFile2 = writeDataFile(table2, "data-1", ImmutableList.of(row1));

            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // 1. snapshotState for checkpoint#1
            long firstCheckpointId = 1;
            harness.snapshot(firstCheckpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            RowData row2 = SimpleDataUtil.createRowData(2, "world");
            DataFile dataFile3 = writeDataFile(table1, "data-2", ImmutableList.of(row2));
            DataFile dataFile4 = writeDataFile(table2, "data-2", ImmutableList.of(row2));
            harness.processElement(of(dataFile3, table1.name()), ++timestamp);
            harness.processElement(of(dataFile4, table2.name()), ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // 2. snapshotState for checkpoint#2
            long secondCheckpointId = 2;
            harness.snapshot(secondCheckpointId, ++timestamp);
            assertFlinkManifests(2, flinkManifestFolder1);
            assertFlinkManifests(2, flinkManifestFolder2);

            // 3. notifyCheckpointComplete for checkpoint#1
            harness.notifyOfCompletedCheckpoint(firstCheckpointId);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, firstCheckpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, firstCheckpointId);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            // 4. notifyCheckpointComplete for checkpoint#2
            harness.notifyOfCompletedCheckpoint(secondCheckpointId);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1, row2), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1, row2), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, secondCheckpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, secondCheckpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
        }
    }

    @Test
    public void testDisorderedEventsBetweenCheckpoints() throws Exception {
        // It's possible that the two checkpoints happen in the following orders:
        //   1. snapshotState for checkpoint#1;
        //   2. snapshotState for checkpoint#2;
        //   3. notifyCheckpointComplete for checkpoint#2;
        //   4. notifyCheckpointComplete for checkpoint#1;
        long timestamp = 0;

        JobID jobId = new JobID();
        OperatorID operatorId;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            createAlternateTableMock();
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData row1 = SimpleDataUtil.createRowData(1, "hello");
            DataFile dataFile1 = writeDataFile(table1, "data-1", ImmutableList.of(row1));
            DataFile dataFile2 = writeDataFile(table2, "data-1", ImmutableList.of(row1));

            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile1, table2.name()), ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // 1. snapshotState for checkpoint#1
            long firstCheckpointId = 1;
            harness.snapshot(firstCheckpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            RowData row2 = SimpleDataUtil.createRowData(2, "world");
            DataFile dataFile3 = writeDataFile(table1, "data-2", ImmutableList.of(row2));
            DataFile dataFile4 = writeDataFile(table2, "data-2", ImmutableList.of(row2));
            harness.processElement(of(dataFile3, table1.name()), ++timestamp);
            harness.processElement(of(dataFile4, table2.name()), ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // 2. snapshotState for checkpoint#2
            long secondCheckpointId = 2;
            harness.snapshot(secondCheckpointId, ++timestamp);
            assertFlinkManifests(2, flinkManifestFolder1);
            assertFlinkManifests(2, flinkManifestFolder2);

            // 3. notifyCheckpointComplete for checkpoint#2
            harness.notifyOfCompletedCheckpoint(secondCheckpointId);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1, row2), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1, row2), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, secondCheckpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, secondCheckpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            // 4. notifyCheckpointComplete for checkpoint#1
            harness.notifyOfCompletedCheckpoint(firstCheckpointId);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1, row2), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1, row2), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, secondCheckpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, secondCheckpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
        }
    }

    @Test
    public void testRecoveryFromValidSnapshot() throws Exception {
        long checkpointId = 0;
        long timestamp = 0;
        List<RowData> expectedRows = Lists.newArrayList();
        OperatorSubtaskState snapshot;

        JobID jobId = new JobID();
        OperatorID operatorId;
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData row = SimpleDataUtil.createRowData(1, "hello");
            expectedRows.add(row);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-1", ImmutableList.of(row));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-1", ImmutableList.of(row));

            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            snapshot = harness.snapshot(++checkpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            harness.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row), branch);
            assertSnapshotSize(table1, 1);
            assertSnapshotSize(table2, 1);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);
        }

        // Restore from the given snapshot
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.getStreamConfig().setOperatorID(operatorId);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 1);
            assertSnapshotSize(table2, 1);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);

            RowData row = SimpleDataUtil.createRowData(2, "world");
            expectedRows.add(row);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-2", ImmutableList.of(row));
            DataFile dataFile2 = writeDataFile(table1, "data-table2-2", ImmutableList.of(row));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);

            harness.snapshot(++checkpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            harness.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 2);
            assertSnapshotSize(table2, 2);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);
        }
    }

    @Test
    public void testRecoveryFromSnapshotWithoutCompletedNotification() throws Exception {
        // We've two steps in checkpoint: 1. snapshotState(ckp); 2. notifyCheckpointComplete(ckp). It's
        // possible that we
        // flink job will restore from a checkpoint with only step#1 finished.
        long checkpointId = 0;
        long timestamp = 0;
        OperatorSubtaskState snapshot;
        List<RowData> expectedRows = Lists.newArrayList();
        JobID jobId = new JobID();
        OperatorID operatorId;
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData row = SimpleDataUtil.createRowData(1, "hello");
            expectedRows.add(row);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-1", ImmutableList.of(row));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-1", ImmutableList.of(row));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);

            snapshot = harness.snapshot(++checkpointId, ++timestamp);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);
        }

        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.getStreamConfig().setOperatorID(operatorId);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            // All flink manifests should be cleaned because it has committed the unfinished iceberg
            // transaction.
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);

            harness.snapshot(++checkpointId, ++timestamp);
            // Did not write any new record, so it won't generate new manifest.
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            harness.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 2);
            assertSnapshotSize(table2, 2);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);

            RowData row = SimpleDataUtil.createRowData(2, "world");
            expectedRows.add(row);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-2", ImmutableList.of(row));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-2", ImmutableList.of(row));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);

            snapshot = harness.snapshot(++checkpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);
        }

        // Redeploying flink job from external checkpoint.
        JobID newJobId = new JobID();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness =
                     createStreamSink(newJobId)) {
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            // All flink manifests should be cleaned because it has committed the unfinished iceberg
            // transaction.
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            assertMaxCommittedCheckpointId(table1, newJobId, operatorId, -1);
            assertMaxCommittedCheckpointId(table2, newJobId, operatorId, -1);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);
            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 3);
            assertSnapshotSize(table2, 3);

            RowData row = SimpleDataUtil.createRowData(3, "foo");
            expectedRows.add(row);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-3", ImmutableList.of(row));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-3", ImmutableList.of(row));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);

            harness.snapshot(++checkpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            harness.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 4);
            assertSnapshotSize(table2, 4);
            assertMaxCommittedCheckpointId(table1, newJobId, operatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, newJobId, operatorId, checkpointId);
        }
    }

    @Test
    public void testStartAnotherJobToWriteSameTable() throws Exception {
        long checkpointId = 0;
        long timestamp = 0;
        List<RowData> rows = Lists.newArrayList();
        List<RowData> tableRows = Lists.newArrayList();

        JobID oldJobId = new JobID();
        OperatorID oldOperatorId;
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness =
                     createStreamSink(oldJobId)) {
            harness.setup();
            harness.open();
            oldOperatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);
            assertMaxCommittedCheckpointId(table1, oldJobId, oldOperatorId, -1L);
            assertMaxCommittedCheckpointId(table2, oldJobId, oldOperatorId, -1L);

            for (int i = 1; i <= 3; i++) {
                rows.add(SimpleDataUtil.createRowData(i, "hello" + i));
                tableRows.addAll(rows);

                DataFile dataFile1 = writeDataFile(table1, String.format("data-table1-%d", i), rows);
                DataFile dataFile2 = writeDataFile(table1, String.format("data-table2-%d", i), rows);
                harness.processElement(of(dataFile1, table1.name()), ++timestamp);
                harness.processElement(of(dataFile2, table2.name()), ++timestamp);
                harness.snapshot(++checkpointId, ++timestamp);
                assertFlinkManifests(1, flinkManifestFolder1);
                assertFlinkManifests(1, flinkManifestFolder2);


                harness.notifyOfCompletedCheckpoint(checkpointId);
                assertFlinkManifests(0, flinkManifestFolder1);
                assertFlinkManifests(0, flinkManifestFolder2);

                SimpleDataUtil.assertTableRows(table1, tableRows, branch);
                SimpleDataUtil.assertTableRows(table2, tableRows, branch);
                assertSnapshotSize(table1, i);
                assertSnapshotSize(table2, i);
                assertMaxCommittedCheckpointId(table1, oldJobId, oldOperatorId, checkpointId);
                assertMaxCommittedCheckpointId(table2, oldJobId, oldOperatorId, checkpointId);
            }
        }

        // The new started job will start with checkpoint = 1 again.
        checkpointId = 0;
        timestamp = 0;
        JobID newJobId = new JobID();
        OperatorID newOperatorId;
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness =
                     createStreamSink(newJobId)) {
            harness.setup();
            harness.open();
            newOperatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 3);
            assertSnapshotSize(table2, 3);
            assertMaxCommittedCheckpointId(table1, oldJobId, oldOperatorId, 3);
            assertMaxCommittedCheckpointId(table1, newJobId, newOperatorId, -1);
            assertMaxCommittedCheckpointId(table2, oldJobId, oldOperatorId, 3);
            assertMaxCommittedCheckpointId(table2, newJobId, newOperatorId, -1);

            rows.add(SimpleDataUtil.createRowData(2, "world"));
            tableRows.addAll(rows);

            DataFile dataFile1 = writeDataFile(table1, "data-table1-new-1", rows);
            DataFile dataFile2 = writeDataFile(table2, "data-table1-new-1", rows);
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            harness.snapshot(++checkpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            harness.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
            SimpleDataUtil.assertTableRows(table1, tableRows, branch);
            SimpleDataUtil.assertTableRows(table2, tableRows, branch);
            assertSnapshotSize(table1, 4);
            assertSnapshotSize(table2, 4);
            assertMaxCommittedCheckpointId(table1, newJobId, newOperatorId, checkpointId);
            assertMaxCommittedCheckpointId(table2, newJobId, newOperatorId, checkpointId);
        }
    }

    @Test
    public void testMultipleJobsWriteSameTable() throws Exception {
        long timestamp = 0;
        List<RowData> tableRows = Lists.newArrayList();

        JobID[] jobs = new JobID[] {new JobID(), new JobID(), new JobID()};
        OperatorID[] operatorIds =
                new OperatorID[] {new OperatorID(), new OperatorID(), new OperatorID()};
        createAlternateTableMock();
        for (int i = 0; i < 20; i++) {
            int jobIndex = i % 3;
            int checkpointId = i / 3;
            JobID jobId = jobs[jobIndex];
            OperatorID operatorId = operatorIds[jobIndex];
            try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
                harness.getStreamConfig().setOperatorID(operatorId);
                harness.setup();
                harness.open();

                assertSnapshotSize(table1, i);
                assertSnapshotSize(table2, i);
                assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId == 0 ? -1 : checkpointId);
                assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId == 0 ? -1 : checkpointId);

                List<RowData> rows = Lists.newArrayList(SimpleDataUtil.createRowData(i, "word-" + i));
                tableRows.addAll(rows);

                DataFile dataFile1 = writeDataFile(table1, String.format("data-table1-%d", i), rows);
                DataFile dataFile2 = writeDataFile(table2, String.format("data-table1-%d", i), rows);
                harness.processElement(of(dataFile1, table1.name()), ++timestamp);
                harness.processElement(of(dataFile2, table2.name()), ++timestamp);
                harness.snapshot(checkpointId + 1, ++timestamp);
                assertFlinkManifests(1, flinkManifestFolder1);
                assertFlinkManifests(1, flinkManifestFolder2);

                harness.notifyOfCompletedCheckpoint(checkpointId + 1);
                assertFlinkManifests(0, flinkManifestFolder1);
                assertFlinkManifests(0, flinkManifestFolder2);
                SimpleDataUtil.assertTableRows(table1, tableRows, branch);
                SimpleDataUtil.assertTableRows(table2, tableRows, branch);
                assertSnapshotSize(table1, i + 1);
                assertSnapshotSize(table2, i + 1);
                assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId + 1);
                assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId + 1);
            }
        }
    }

    @Test
    public void testMultipleSinksRecoveryFromValidSnapshot() throws Exception {
        long checkpointId = 0;
        long timestamp = 0;
        List<RowData> expectedRows = Lists.newArrayList();
        OperatorSubtaskState snapshot1;
        OperatorSubtaskState snapshot2;

        JobID jobId = new JobID();
        OperatorID operatorId1 = new OperatorID();
        OperatorID operatorId2 = new OperatorID();
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness1 = createStreamSink(jobId);
             OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness2 = createStreamSink(jobId)) {
            harness1.getStreamConfig().setOperatorID(operatorId1);
            harness1.setup();
            harness1.open();
            harness2.getStreamConfig().setOperatorID(operatorId2);
            harness2.setup();
            harness2.open();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId1, -1L);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId2, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId1, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId2, -1L);

            RowData row1 = SimpleDataUtil.createRowData(1, "hello1");
            expectedRows.add(row1);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-1-1", ImmutableList.of(row1));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-1-1", ImmutableList.of(row1));

            harness1.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness1.processElement(of(dataFile2, table2.name()), ++timestamp);
            snapshot1 = harness1.snapshot(++checkpointId, ++timestamp);

            RowData row2 = SimpleDataUtil.createRowData(1, "hello2");
            expectedRows.add(row2);
            DataFile dataFile3 = writeDataFile(table1, "data-table1-1-2", ImmutableList.of(row2));
            DataFile dataFile4 = writeDataFile(table2, "data-table2-1-2", ImmutableList.of(row2));

            harness2.processElement(of(dataFile3, table1.name()), ++timestamp);
            harness2.processElement(of(dataFile4, table2.name()), ++timestamp);
            snapshot2 = harness2.snapshot(checkpointId, ++timestamp);
            assertFlinkManifests(2, flinkManifestFolder1);
            assertFlinkManifests(2, flinkManifestFolder2);

            // Only notify one of the committers
            harness1.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            // Only the first row is committed at this point
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1), branch);
            assertSnapshotSize(table1, 1);
            assertSnapshotSize(table2, 1);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId1, checkpointId);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId2, -1);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId1, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId2, -1);
        }

        // Restore from the given snapshot
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness1 = createStreamSink(jobId);
             OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness2 = createStreamSink(jobId)) {
            harness1.getStreamConfig().setOperatorID(operatorId1);
            harness1.setup();
            harness1.initializeState(snapshot1);
            harness1.open();

            harness2.getStreamConfig().setOperatorID(operatorId2);
            harness2.setup();
            harness2.initializeState(snapshot2);
            harness2.open();

            // All flink manifests should be cleaned because it has committed the unfinished iceberg
            // transaction.
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 2);
            assertSnapshotSize(table2, 2);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId1, checkpointId);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId2, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId1, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId2, checkpointId);

            RowData row1 = SimpleDataUtil.createRowData(2, "world1");
            expectedRows.add(row1);
            DataFile dataFile1 = writeDataFile(table1, "data-table1-2-1", ImmutableList.of(row1));
            DataFile dataFile2 = writeDataFile(table2, "data-table1-2-1", ImmutableList.of(row1));

            harness1.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness1.processElement(of(dataFile2, table2.name()), ++timestamp);
            harness1.snapshot(++checkpointId, ++timestamp);

            RowData row2 = SimpleDataUtil.createRowData(2, "world2");
            expectedRows.add(row2);
            DataFile dataFile3 = writeDataFile(table1, "data-table1-2-2", ImmutableList.of(row2));
            DataFile dataFile4 = writeDataFile(table2, "data-table2-2-2", ImmutableList.of(row2));
            harness2.processElement(of(dataFile3, table1.name()), ++timestamp);
            harness2.processElement(of(dataFile4, table2.name()), ++timestamp);
            harness2.snapshot(checkpointId, ++timestamp);

            assertFlinkManifests(2, flinkManifestFolder1);
            assertFlinkManifests(2, flinkManifestFolder2);

            harness1.notifyOfCompletedCheckpoint(checkpointId);
            harness2.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, expectedRows, branch);
            SimpleDataUtil.assertTableRows(table2, expectedRows, branch);
            assertSnapshotSize(table1, 4);
            assertSnapshotSize(table2, 4);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId1, checkpointId);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId2, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId1, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId2, checkpointId);
        }
    }

    @Test
    public void testBoundedStream() throws Exception {
        JobID jobId = new JobID();
        OperatorID operatorId;
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
            assertSnapshotSize(table1, 0);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertSnapshotSize(table2, 0);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            List<RowData> tableRows = Lists.newArrayList(SimpleDataUtil.createRowData(1, "word-1"));

            DataFile dataFile1 = writeDataFile(table1, "data-table1-1", tableRows);
            DataFile dataFile2 = writeDataFile(table2, "data-table2-1", tableRows);
            harness.processElement(of(dataFile1, table1.name()), 1);
            harness.processElement(of(dataFile2, table2.name()), 1);
            ((BoundedOneInput) harness.getOneInputOperator()).endInput();

            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
            SimpleDataUtil.assertTableRows(table1, tableRows, branch);
            SimpleDataUtil.assertTableRows(table2, tableRows, branch);
            assertSnapshotSize(table1, 1);
            assertSnapshotSize(table2, 1);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, Long.MAX_VALUE);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, Long.MAX_VALUE);
            Assert.assertEquals(
                    TestIcebergMultiTableFilesCommitter.class.getName(),
                    SimpleDataUtil.latestSnapshot(table1, branch).summary().get("flink.test"));
            Assert.assertEquals(
                    TestIcebergMultiTableFilesCommitter.class.getName(),
                    SimpleDataUtil.latestSnapshot(table2, branch).summary().get("flink.test"));
        }
    }

    @Test
    public void testFlinkManifests() throws Exception {
        long timestamp = 0;
        final long checkpoint = 10;

        JobID jobId = new JobID();
        OperatorID operatorId;
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData row1 = SimpleDataUtil.createRowData(1, "hello");
            DataFile dataFile1 = writeDataFile(table1, "data-table1-1", ImmutableList.of(row1));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-1", ImmutableList.of(row1));

            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // 1. snapshotState for checkpoint#1
            harness.snapshot(checkpoint, ++timestamp);
            List<Path> manifestPaths1 = assertFlinkManifests(1, flinkManifestFolder1);
            List<Path> manifestPaths2 = assertFlinkManifests(1, flinkManifestFolder2);
            Path manifestPath1 = manifestPaths1.get(0);
            Assert.assertEquals(
                    "File name should have the expected pattern.",
                    String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1),
                    manifestPath1.getFileName().toString());
            Assert.assertEquals(
                    "File name should have the expected pattern.",
                    String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1),
                    manifestPaths2.get(0).getFileName().toString());

            // 2. Read the data files from manifests and assert.
            List<DataFile> dataFiles1 =
                    FlinkManifestUtil.readDataFiles(
                            createTestingManifestFile(manifestPath1), table1.io(), table1.specs());
            List<DataFile> dataFiles2 =
                    FlinkManifestUtil.readDataFiles(
                            createTestingManifestFile(manifestPaths2.get(0)), table2.io(), table2.specs());
            Assert.assertEquals(1, dataFiles1.size());
            Assert.assertEquals(1, dataFiles2.size());
            TestHelpers.assertEquals(dataFile1, dataFiles1.get(0));
            TestHelpers.assertEquals(dataFile2, dataFiles2.get(0));

            // 3. notifyCheckpointComplete for checkpoint#1
            harness.notifyOfCompletedCheckpoint(checkpoint);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpoint);
            assertFlinkManifests(0, flinkManifestFolder1);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1), branch);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpoint);
            assertFlinkManifests(0, flinkManifestFolder2);
        }
    }

    @Test
    public void testDeleteFiles() throws Exception {
        Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);

        long timestamp = 0;
        long checkpoint = 10;

        JobID jobId = new JobID();
        OperatorID operatorId;
        FileAppenderFactory<RowData> appenderFactory1 = createDeletableAppenderFactory(table1);
        FileAppenderFactory<RowData> appenderFactory2 = createDeletableAppenderFactory(table2);
        createAlternateTableMock();

        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData row1 = SimpleDataUtil.createInsert(1, "aaa");
            DataFile dataFile1 = writeDataFile(table1, "data-table1-file-1", ImmutableList.of(row1));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-file-1", ImmutableList.of(row1));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            // 1. snapshotState for checkpoint#1
            harness.snapshot(checkpoint, ++timestamp);
            List<Path> manifestPaths1 = assertFlinkManifests(1, flinkManifestFolder1);
            List<Path> manifestPaths2 = assertFlinkManifests(1, flinkManifestFolder2);
            Path manifestPath = manifestPaths1.get(0);
            Assert.assertEquals(
                    "File name should have the expected pattern.",
                    String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1),
                    manifestPath.getFileName().toString());
            Assert.assertEquals(
                    "File name should have the expected pattern.",
                    String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1),
                    manifestPaths2.get(0).getFileName().toString());

            // 2. Read the data files from manifests and assert.
            List<DataFile> dataFiles1 =
                    FlinkManifestUtil.readDataFiles(
                            createTestingManifestFile(manifestPath), table1.io(), table1.specs());
            Assert.assertEquals(1, dataFiles1.size());
            TestHelpers.assertEquals(dataFile1, dataFiles1.get(0));

            List<DataFile> dataFiles2 =
                    FlinkManifestUtil.readDataFiles(
                            createTestingManifestFile(manifestPath), table2.io(), table2.specs());
            Assert.assertEquals(1, dataFiles2.size());
            TestHelpers.assertEquals(dataFile1, dataFiles2.get(0));

            // 3. notifyCheckpointComplete for checkpoint#1
            harness.notifyOfCompletedCheckpoint(checkpoint);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row1), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row1), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpoint);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpoint);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            // 4. process both data files and delete files.
            RowData row2 = SimpleDataUtil.createInsert(2, "bbb");
            DataFile dataFile3 = writeDataFile(table1, "data-table1-file-2", ImmutableList.of(row2));
            DataFile dataFile4 = writeDataFile(table2, "data-table1-file-2", ImmutableList.of(row2));

            RowData delete1 = SimpleDataUtil.createDelete(1, "aaa");
            DeleteFile deleteFile1 =
                    writeEqDeleteFile(appenderFactory1, "delete-table1-file-1", ImmutableList.of(delete1), table1);
            DeleteFile deleteFile2 =
                    writeEqDeleteFile(appenderFactory2, "delete-table2-file-1", ImmutableList.of(delete1), table2);
            harness.processElement(
                    of(dataFile3, deleteFile1, table1.name()),
                    ++timestamp);
            harness.processElement(
                    of(dataFile4, deleteFile2, table2.name()),
                    ++timestamp);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpoint);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpoint);

            // 5. snapshotState for checkpoint#2
            harness.snapshot(++checkpoint, ++timestamp);
            assertFlinkManifests(2, flinkManifestFolder1);
            assertFlinkManifests(2, flinkManifestFolder2);

            // 6. notifyCheckpointComplete for checkpoint#2
            harness.notifyOfCompletedCheckpoint(checkpoint);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(row2), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(row2), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpoint);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpoint);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
        }
    }

    @Test
    public void testCommitTwoCheckpointsInSingleTxn() throws Exception {
        Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);

        long timestamp = 0;
        long checkpoint = 10;

        JobID jobId = new JobID();
        OperatorID operatorId;
        FileAppenderFactory<RowData> appenderFactory1 = createDeletableAppenderFactory(table1);
        FileAppenderFactory<RowData> appenderFactory2 = createDeletableAppenderFactory(table2);
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertMaxCommittedCheckpointId(table1, jobId, operatorId, -1L);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, -1L);

            RowData insert1 = SimpleDataUtil.createInsert(1, "aaa");
            RowData insert2 = SimpleDataUtil.createInsert(2, "bbb");
            RowData delete3 = SimpleDataUtil.createDelete(3, "ccc");
            DataFile dataFile1 = writeDataFile(table1, "data-table1-file-1", ImmutableList.of(insert1, insert2));
            DataFile dataFile2 = writeDataFile(table2, "data-table2-file-1", ImmutableList.of(insert1, insert2));
            DeleteFile deleteFile1 =
                    writeEqDeleteFile(appenderFactory1, "delete-file-1", ImmutableList.of(delete3), table1);
            DeleteFile deleteFile2 =
                    writeEqDeleteFile(appenderFactory2, "delete-file-1", ImmutableList.of(delete3), table2);
            harness.processElement(
                    of(dataFile1, deleteFile1, table1.name()),
                    ++timestamp);
            harness.processElement(
                    of(dataFile2, deleteFile2, table2.name()),
                    ++timestamp);

            // The 1th snapshotState.
            harness.snapshot(checkpoint, ++timestamp);

            RowData insert4 = SimpleDataUtil.createInsert(4, "ddd");
            RowData delete2 = SimpleDataUtil.createDelete(2, "bbb");
            DataFile dataFile3 = writeDataFile(table1, "data-table1-file-2", ImmutableList.of(insert4));
            DataFile dataFile4 = writeDataFile(table2, "data-table2-file-2", ImmutableList.of(insert4));
            DeleteFile deleteFile3 =
                    writeEqDeleteFile(appenderFactory1, "delete-file-2", ImmutableList.of(delete2), table1);
            DeleteFile deleteFile4 =
                    writeEqDeleteFile(appenderFactory2, "delete-file-2", ImmutableList.of(delete2), table2);
            harness.processElement(
                    of(dataFile3, deleteFile3, table1.name()),
                    ++timestamp);
            harness.processElement(
                    of(dataFile4, deleteFile4, table2.name()),
                    ++timestamp);

            // The 2nd snapshotState.
            harness.snapshot(++checkpoint, ++timestamp);

            // Notify the 2nd snapshot to complete.
            harness.notifyOfCompletedCheckpoint(checkpoint);
            SimpleDataUtil.assertTableRows(table1, ImmutableList.of(insert1, insert4), branch);
            SimpleDataUtil.assertTableRows(table2, ImmutableList.of(insert1, insert4), branch);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpoint);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpoint);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);
            Assert.assertEquals(
                    "Should have committed 2 txn.", 2, ImmutableList.copyOf(table1.snapshots()).size());
            Assert.assertEquals(
                    "Should have committed 2 txn.", 2, ImmutableList.copyOf(table2.snapshots()).size());
        }
    }

    @Test
    public void testSpecEvolution() throws Exception {
        long timestamp = 0;
        int checkpointId = 0;
        List<RowData> rows = Lists.newArrayList();
        JobID jobId = new JobID();

        OperatorID operatorId;
        OperatorSubtaskState snapshot;
        DataFile dataFile1;
        DataFile dataFile2;
        int specId1, specId2;
        createAlternateTableMock();
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.setup();
            harness.open();
            operatorId = harness.getOperator().getOperatorID();

            assertSnapshotSize(table1, 0);
            assertSnapshotSize(table2, 0);

            checkpointId++;
            RowData rowData = SimpleDataUtil.createRowData(checkpointId, "hello" + checkpointId);
            // table unpartitioned
            dataFile1 = writeDataFile(table1, "data-table1-" + checkpointId, ImmutableList.of(rowData));
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            dataFile2 = writeDataFile(table2, "data-table2-" + checkpointId, ImmutableList.of(rowData));
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            rows.add(rowData);
            harness.snapshot(checkpointId, ++timestamp);

            specId1 =
                    getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId, TableIdentifier.of("dummy", table1.name()));
            assertThat(specId1).isEqualTo(table1.spec().specId());

            specId2 =
                    getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId, TableIdentifier.of("dummy", table2.name()));
            assertThat(specId2).isEqualTo(table2.spec().specId());

            harness.notifyOfCompletedCheckpoint(checkpointId);

            // Change partition spec
            table1.refresh();
            PartitionSpec oldSpec1 = table1.spec();
            table1.updateSpec().addField("id").commit();

            table2.refresh();
            PartitionSpec oldSpec2 = table2.spec();
            table2.updateSpec().addField("id").commit();

            checkpointId++;
            rowData = SimpleDataUtil.createRowData(checkpointId, "hello" + checkpointId);
            // write data with old partition spec
            StructLike partition1 = new PartitionData(table1.spec().partitionType());
            partition1.set(0, checkpointId);
            StructLike partition2 = new PartitionData(table2.spec().partitionType());
            partition2.set(0, checkpointId);
            dataFile1 = writeDataFile("data-table1-" + checkpointId, ImmutableList.of(rowData), table1.spec(), partition1, table1);
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            dataFile2 = writeDataFile("data-table2-" + checkpointId, ImmutableList.of(rowData), table2.spec(), partition2, table2);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            rows.add(rowData);
            snapshot = harness.snapshot(checkpointId, ++timestamp);

            specId1 =
                    getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId, TableIdentifier.of("dummy", table1.name()));
            assertThat(specId1).isEqualTo(table1.spec().specId());
            specId2 =
                    getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId, TableIdentifier.of("dummy", table2.name()));
            assertThat(specId2).isEqualTo(table2.spec().specId());

            harness.notifyOfCompletedCheckpoint(checkpointId);

            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, rows, branch);
            assertSnapshotSize(table1, checkpointId);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            SimpleDataUtil.assertTableRows(table2, rows, branch);
            assertSnapshotSize(table2, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);
        }

        // Restore from the given snapshot
        try (OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> harness = createStreamSink(jobId)) {
            harness.getStreamConfig().setOperatorID(operatorId);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            SimpleDataUtil.assertTableRows(table1, rows, branch);
            assertSnapshotSize(table1, checkpointId);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            SimpleDataUtil.assertTableRows(table2, rows, branch);
            assertSnapshotSize(table2, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);

            checkpointId++;
            RowData row = SimpleDataUtil.createRowData(checkpointId, "world" + checkpointId);
            StructLike partition1 = new PartitionData(table1.spec().partitionType());
            partition1.set(0, checkpointId);
            dataFile1 =
                    writeDataFile("data-table1-" + checkpointId, ImmutableList.of(row), table1.spec(), partition1, table1);
            harness.processElement(of(dataFile1, table1.name()), ++timestamp);
            rows.add(row);
            StructLike partition2 = new PartitionData(table2.spec().partitionType());
            partition2.set(0, checkpointId);
            dataFile2 =
                    writeDataFile("data-table2-" + checkpointId, ImmutableList.of(row), table2.spec(), partition2, table2);
            harness.processElement(of(dataFile2, table2.name()), ++timestamp);
            harness.snapshot(checkpointId, ++timestamp);
            assertFlinkManifests(1, flinkManifestFolder1);
            assertFlinkManifests(1, flinkManifestFolder2);

            specId1 =
                    getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId, TableIdentifier.of("dummy", table1.name()));
            assertThat(specId1).isEqualTo(table1.spec().specId());

            specId2 =
                    getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId, TableIdentifier.of("dummy", table2.name()));
            assertThat(specId2).isEqualTo(table2.spec().specId());

            harness.notifyOfCompletedCheckpoint(checkpointId);
            assertFlinkManifests(0, flinkManifestFolder1);
            assertFlinkManifests(0, flinkManifestFolder2);

            SimpleDataUtil.assertTableRows(table1, rows, branch);
            assertSnapshotSize(table1, checkpointId);
            assertMaxCommittedCheckpointId(table1, jobId, operatorId, checkpointId);
            SimpleDataUtil.assertTableRows(table2, rows, branch);
            assertSnapshotSize(table2, checkpointId);
            assertMaxCommittedCheckpointId(table2, jobId, operatorId, checkpointId);
        }
    }

    private DataFile writeDataFile(
            String filename, List<RowData> rows, PartitionSpec spec, StructLike partition, Table table)
            throws IOException {
        return SimpleDataUtil.writeFile(
                table,
                table.schema(),
                spec,
                CONF,
                table.location(),
                format.addExtension(filename),
                rows,
                partition);
    }

    private int getStagingManifestSpecId(OperatorStateStore operatorStateStore, long checkPointId, TableIdentifier tableIdentifier)
            throws Exception {
        ListState<Map<TableIdentifier, SortedMap<Long, byte[]>>> checkpointsState =
                operatorStateStore.getListState(IcebergMultiTableFilesCommitter.buildStateDescriptor());
        Map<TableIdentifier, SortedMap<Long, byte[]>> tableDataFilesMap =
                Maps.newHashMap(checkpointsState.get().iterator().next());
        SortedMap<Long, byte[]> statedDataFiles = tableDataFilesMap.get(tableIdentifier);
        DeltaManifests deltaManifests =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        DeltaManifestsSerializer.INSTANCE, statedDataFiles.get(checkPointId));
        return deltaManifests.dataManifest().partitionSpecId();
    }

    private DeleteFile writeEqDeleteFile(
            FileAppenderFactory<RowData> appenderFactory, String filename, List<RowData> deletes, Table table)
            throws IOException {
        return SimpleDataUtil.writeEqDeleteFile(table, format, filename, appenderFactory, deletes);
    }

    private FileAppenderFactory<RowData> createDeletableAppenderFactory(Table table) {
        int[] equalityFieldIds =
                new int[] {
                        table.schema().findField("id").fieldId(), table.schema().findField("data").fieldId()
                };
        return new FlinkAppenderFactory(
                table,
                table.schema(),
                FlinkSchemaUtil.convert(table.schema()),
                table.properties(),
                table.spec(),
                equalityFieldIds,
                table.schema(),
                null);
    }

    private ManifestFile createTestingManifestFile(Path manifestPath) {
        return new GenericManifestFile(
                manifestPath.toAbsolutePath().toString(),
                manifestPath.toFile().length(),
                0,
                ManifestContent.DATA,
                0,
                0,
                0L,
                0,
                0,
                0,
                0,
                0,
                0,
                null,
                null);
    }

    private void createAlternateTableMock() {
        Mockito.when(tableLoader.loadTable()).thenAnswer(new Answer<Table>() {
            private int invocationCount = 0;
            @Override
            public Table answer(InvocationOnMock invocation) throws Throwable {
                invocationCount++;
                if(invocationCount % 2 == 1) {
                    return table1;
                }
                else {
                    return table2;
                }
            }
        });
    }

    private DataFile writeDataFile(Table table, String filename, List<RowData> rows) throws IOException {
        return SimpleDataUtil.writeFile(
                table,
                table.schema(),
                table.spec(),
                CONF,
                table.location(),
                format.addExtension(filename),
                rows);
    }

    protected TestTables.TestTable create(File tableDir, String name, Schema schema, PartitionSpec spec) {
        return TestTables.create(tableDir, name, schema, spec, formatVersion);
    }

    private OneInputStreamOperatorTestHarness<TableAwareWriteResult, Void> createStreamSink(JobID jobID)
            throws Exception {
        TestOperatorFactory factory = TestOperatorFactory.of(branch, catalogLoader);
        return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
    }

    private List<Path> assertFlinkManifests(int expectedCount, File flinkManifestFolder) throws IOException {
        List<Path> manifests =
                Files.list(flinkManifestFolder.toPath())
                        .filter(p -> !p.toString().endsWith(".crc"))
                        .collect(Collectors.toList());
        Assert.assertEquals(
                String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
                expectedCount,
                manifests.size());
        return manifests;
    }

    private void assertMaxCommittedCheckpointId(Table table, JobID jobID, OperatorID operatorID, long expectedId) {
        table.refresh();
        long actualId =
                IcebergFilesCommitter.getMaxCommittedCheckpointId(
                        table, jobID.toString(), operatorID.toHexString(), branch);
        Assert.assertEquals(expectedId, actualId);
    }

    private void assertSnapshotSize(Table table, int expectedSnapshotSize) {
        table.refresh();
        Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
    }

    private static MockEnvironment createEnvironment(JobID jobID) {
        return new MockEnvironmentBuilder()
                .setTaskName("test task")
                .setManagedMemorySize(32 * 1024)
                .setInputSplitProvider(new MockInputSplitProvider())
                .setBufferSize(256)
                .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
                .setExecutionConfig(new ExecutionConfig())
                .setMaxParallelism(16)
                .setJobID(jobID)
                .build();
    }

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<Void>
            implements OneInputStreamOperatorFactory<TableAwareWriteResult, Void> {
        private final String branch;
        private CatalogLoader catalogLoader;

        private TestOperatorFactory(String branch, CatalogLoader catalogLoader) {
            this.branch = branch;
            this.catalogLoader = catalogLoader;
        }

        private static TestOperatorFactory of( String branch, CatalogLoader catalogLoader) {
            return new TestOperatorFactory(branch, catalogLoader);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Void>> T createStreamOperator(
                StreamOperatorParameters<Void> param) {
            IcebergMultiTableFilesCommitter committer =
                    new IcebergMultiTableFilesCommitter(
                            catalogLoader,
                            false,
                            Collections.singletonMap("flink.test", TestIcebergMultiTableFilesCommitter.class.getName()),
                            ThreadPools.WORKER_THREAD_POOL_SIZE,
                            branch);
            committer.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
            return (T) committer;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return IcebergMultiTableFilesCommitter.class;
        }
    }

}