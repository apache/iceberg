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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.TableAwareWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ThreadPools;
import org.junit.After;
import org.junit.Assert;
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
import java.util.stream.Collectors;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.IcebergFilesCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

@RunWith(Parameterized.class)
public class TestIcebergMultiTableFileCommitter {

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

    public TestIcebergMultiTableFileCommitter(String format, int formatVersion, String branch) {
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
                        TestIcebergMultiTableFileCommitter.class.getName(),
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
                        TestIcebergMultiTableFileCommitter.class.getName(),
                        SimpleDataUtil.latestSnapshot(table1, branch).summary().get("flink.test"));
                Assert.assertEquals(
                        TestIcebergMultiTableFileCommitter.class.getName(),
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
            IcebergMultiTableFileCommitter committer =
                    new IcebergMultiTableFileCommitter(
                            catalogLoader,
                            false,
                            Collections.singletonMap("flink.test", TestIcebergMultiTableFileCommitter.class.getName()),
                            ThreadPools.WORKER_THREAD_POOL_SIZE,
                            branch);
            committer.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
            return (T) committer;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return IcebergMultiTableFileCommitter.class;
        }
    }

}