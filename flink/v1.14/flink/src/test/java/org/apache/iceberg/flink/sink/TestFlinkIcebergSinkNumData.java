/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSinkNumData extends TableTestBase {
    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            MiniClusterResource.createWithClassloaderCheckDisabled();

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final int FORMAT_V2 = 2;
    private static final TypeInformation<Row> ROW_TYPE_INFO =
            new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA_NUM_TYPE.getFieldTypes());

    private static final Map<String, RowKind> ROW_KIND_MAP = ImmutableMap.of(
            "+I", RowKind.INSERT,
            "-D", RowKind.DELETE,
            "-U", RowKind.UPDATE_BEFORE,
            "+U", RowKind.UPDATE_AFTER);

    private static final int ROW_ID_POS = 0;

    private final FileFormat format;
    private final int parallelism;
    private final boolean partitioned;

    private StreamExecutionEnvironment env;
    private TestTableLoader tableLoader;

    public TestFlinkIcebergSinkNumData(String format, int parallelism, boolean partitioned) {
        super(FORMAT_V2);
        this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
        this.parallelism = parallelism;
        this.partitioned = partitioned;
    }

    @Parameterized.Parameters(name = "FileFormat = {0}, Parallelism = {1}, Partitioned={2}")
    public static Object[][] parameters() {
        return new Object[][]{
                new Object[]{"avro", 1, true},
                new Object[]{"avro", 1, false},
                new Object[]{"avro", 2, true},
                new Object[]{"avro", 2, false},
                new Object[]{"orc", 1, true},
                new Object[]{"orc", 1, false},
                new Object[]{"orc", 2, true},
                new Object[]{"orc", 2, false},
                new Object[]{"parquet", 1, true},
                new Object[]{"parquet", 1, false},
                new Object[]{"parquet", 2, true},
                new Object[]{"parquet", 2, false}
        };
    }

    @Override
    @Before
    public void setupTable() throws IOException {
        this.tableDir = temp.newFolder();
        this.metadataDir = new File(tableDir, "metadata");
        Assert.assertTrue(tableDir.delete());

        if (!partitioned) {
            table = create(SimpleDataUtil.SCHEMA_NUM_TYPE, PartitionSpec.unpartitioned());
        } else {
            table = create(SimpleDataUtil.SCHEMA_NUM_TYPE,
                    PartitionSpec.builderFor(SimpleDataUtil.SCHEMA_NUM_TYPE).identity("id").build());
        }

        table.updateProperties()
                .set(TableProperties.DEFAULT_FILE_FORMAT, format.name())
                .commit();

        env = StreamExecutionEnvironment.getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
                .enableCheckpointing(100L)
                .setParallelism(parallelism)
                .setMaxParallelism(parallelism);

        tableLoader = new TestTableLoader(tableDir.getAbsolutePath());
    }

    private List<Snapshot> findValidSnapshots(Table table) {
        List<Snapshot> validSnapshots = Lists.newArrayList();
        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.allManifests().stream().anyMatch(m -> snapshot.snapshotId() == m.snapshotId())) {
                validSnapshots.add(snapshot);
            }
        }
        return validSnapshots;
    }

    @Test
    public void testWriteMaxValue() throws Exception {
        List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
                ImmutableList.of(
                        row("+I", 1, Integer.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE, Long.MAX_VALUE,
                                1643811742000L, 1643811742000L, 1643811742000L, 10.24d),
                        row("-D", 1, Integer.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE, Long.MAX_VALUE,
                                1643811742000L, 1643811742000L, 1643811742000L, 10.24d),
                        row("+I", 2, Integer.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE, Long.MAX_VALUE,
                                1643811742000L, 1643811742000L, 1643811742000L, 10.24d)
                )
        );

        List<List<Record>> expectedRecords = ImmutableList.of(
                ImmutableList.of(record(2, Integer.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE, Long.MAX_VALUE,
                        1643811742000L, 1643811742000L, 1643811742000L, 10.24d))
        );

        testWriteDataBase(ImmutableList.of("id"), row -> row.getField(ROW_ID_POS), true,
                elementsPerCheckpoint, expectedRecords);
    }

    @Test
    public void testWriteMinValue() throws Exception {
        List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
                ImmutableList.of(
                        row("+I", 1, Integer.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE, Long.MIN_VALUE,
                                1643811742000L, 1643811742000L, 1643811742000L, 10.24d),
                        row("-D", 1, Integer.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE, Long.MIN_VALUE,
                                1643811742000L, 1643811742000L, 1643811742000L, 10.24d),
                        row("+I", 2, Integer.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE, Long.MIN_VALUE,
                                1643811742000L, 1643811742000L, 1643811742000L, 10.24d)
                )
        );

        List<List<Record>> expectedRecords = ImmutableList.of(
                ImmutableList.of(record(2, Integer.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE, Long.MIN_VALUE,
                        1643811742000L, 1643811742000L, 1643811742000L, 10.24d))
        );

        testWriteDataBase(ImmutableList.of("id"), row -> row.getField(ROW_ID_POS), true,
                elementsPerCheckpoint, expectedRecords);
    }

    private void testWriteDataBase(List<String> equalityFieldColumns,
                                   KeySelector<Row, Object> keySelector,
                                   boolean insertAsUpsert,
                                   List<List<Row>> elementsPerCheckpoint,
                                   List<List<Record>> expectedRecordsPerCheckpoint) throws Exception {
        DataStream<Row> dataStream = env.addSource(new BoundedTestSource<>(elementsPerCheckpoint), ROW_TYPE_INFO);

        dataStream = dataStream.keyBy(keySelector);

        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA_NUM_TYPE)
                .tableLoader(tableLoader)
                .tableSchema(SimpleDataUtil.FLINK_SCHEMA_NUM_TYPE)
                .writeParallelism(parallelism)
                .equalityFieldColumns(equalityFieldColumns)
                .upsert(insertAsUpsert)
                .append();

        // Execute the program.
        env.execute("Test Iceberg NumData.");

        table.refresh();
        List<Snapshot> snapshots = findValidSnapshots(table);
        int expectedSnapshotNum = expectedRecordsPerCheckpoint.size();
        Assert.assertEquals("Should have the expected snapshot number", expectedSnapshotNum, snapshots.size());

        for (int i = 0; i < expectedSnapshotNum; i++) {
            long snapshotId = snapshots.get(i).snapshotId();
            List<Record> expectedRecords = expectedRecordsPerCheckpoint.get(i);
            StructLikeSet expected = expectedRowSet(expectedRecords.toArray(new Record[0]));
            StructLikeSet actual = actualRowSet(snapshotId, "*");
            Assert.assertEquals("Should have the expected records for the checkpoint#" + i, expected, actual);
        }
    }

    private Row row(String rowKind, int id, int intV, float floatV, double doubleV, long date, long time,
                    long timestamp, long bigint, double decimal) {
        RowKind kind = ROW_KIND_MAP.get(rowKind);
        if (kind == null) {
            throw new IllegalArgumentException("Unknown row kind: " + rowKind);
        }

        return Row.ofKind(kind, id, intV, floatV, doubleV, new Date(date).toLocalDate(), new Time(time).toLocalTime(),
                DateTimeUtil.timestampFromMicros(timestamp * 1000), bigint, BigDecimal.valueOf(decimal));
    }

    private Record record(int id, int intV, float floatV, double doubleV, long date, long time, long timestamp,
                          long bigint, double decimal) {
        Record record = GenericRecord.create(SimpleDataUtil.SCHEMA_NUM_TYPE);
        record.setField("id", id);
        record.setField("int", intV);
        record.setField("float", floatV);
        record.setField("double", doubleV);
        record.setField("date", DateTimeUtil.dateFromDays((int) new Date(date).toLocalDate().toEpochDay()));
        record.setField("time", new Time(time).toLocalTime());
        record.setField("timestamp", DateTimeUtil.timestampFromMicros(timestamp * 1000));
        record.setField("bigint", bigint);
        record.setField("decimal", BigDecimal.valueOf(decimal));
        return record;
    }

    private StructLikeSet expectedRowSet(Record... records) {
        StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
        List<Record> recordList = Arrays.asList(records);
        recordList.forEach(record -> set.add(new InternalRecordWrapper(table.schema().asStruct()).wrap(record)));
        return set;
    }

    private StructLikeSet actualRowSet(long snapshotId, String... columns) throws IOException {
        table.refresh();
        StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .select(columns)
                .build()) {
            reader.forEach(record -> set.add(new InternalRecordWrapper(table.schema().asStruct()).wrap(record)));
        }
        return set;
    }
}
