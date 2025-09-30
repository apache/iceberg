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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.flink.TestFixtures.TABLE;
import static org.apache.iceberg.flink.sink.dynamic.DynamicCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test class to compare {@link DynamicIcebergSink} against {@link IcebergSink} to
 * measure and compare their throughput.
 *
 * <p>The test dynamically generates input for multiple tables, then writes to these tables. For the
 * DynamicSink, a single sink is used to write all tables. For the IcebergSink, one sink is used per
 * table. The test logs the written record counts and elapsed time based on the Iceberg snapshot
 * metadata.
 *
 * <h2>Usage</h2>
 *
 * <ul>
 *   <li>Set the SAMPLE_SIZE, RECORD_SIZE, and TABLE_NUM.
 *   <li>Run the unit tests and review logs for performance results.
 * </ul>
 *
 * <p>Note: This test is disabled by default and should be enabled manually when performance testing
 * is needed. It is not intended as a standard unit test.
 */
@Disabled("Please enable manually for performance testing.")
class TestDynamicIcebergSinkPerf {
  private static final Logger LOG = LoggerFactory.getLogger(TestDynamicIcebergSinkPerf.class);

  @RegisterExtension
  protected static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TABLE);

  private static final int SAMPLE_SIZE = 50_000;
  private static final int RECORD_SIZE = 5_000_000;
  private static final int TABLE_NUM = 3;
  private static final int PARALLELISM = 2;
  private static final int WRITE_PARALLELISM = 2;
  private static final TableIdentifier[] IDENTIFIERS = new TableIdentifier[TABLE_NUM];
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name2", Types.StringType.get()),
          Types.NestedField.required(3, "name3", Types.StringType.get()),
          Types.NestedField.required(4, "name4", Types.StringType.get()),
          Types.NestedField.required(5, "name5", Types.StringType.get()),
          Types.NestedField.required(6, "name6", Types.StringType.get()),
          Types.NestedField.required(7, "name7", Types.StringType.get()),
          Types.NestedField.required(8, "name8", Types.StringType.get()),
          Types.NestedField.required(9, "name9", Types.StringType.get()));
  private static final List<Integer> RANGE =
      IntStream.range(0, RECORD_SIZE).boxed().collect(Collectors.toList());

  private static List<DynamicRecord> rows;
  private StreamExecutionEnvironment env;

  @BeforeEach
  void before() {
    for (int i = 0; i < TABLE_NUM; ++i) {
      // So the table name hash difference is bigger than 1
      IDENTIFIERS[i] = TableIdentifier.of(DATABASE, TABLE + "_" + (i * 13));

      Table table =
          CATALOG_EXTENSION
              .catalog()
              .createTable(
                  IDENTIFIERS[i],
                  SCHEMA,
                  PartitionSpec.unpartitioned(),
                  ImmutableMap.of(MAX_CONTINUOUS_EMPTY_COMMITS, "100000"));

      table.manageSnapshots().createBranch("main").commit();
    }

    List<Record> records = RandomGenericData.generate(SCHEMA, SAMPLE_SIZE, 1L);
    rows = Lists.newArrayListWithCapacity(records.size());
    for (int i = 0; i < records.size(); ++i) {
      rows.add(
          new DynamicRecord(
              IDENTIFIERS[i % TABLE_NUM],
              "main",
              SCHEMA,
              RowDataConverter.convert(SCHEMA, records.get(i)),
              PartitionSpec.unpartitioned(),
              DistributionMode.NONE,
              WRITE_PARALLELISM));
    }

    Configuration configuration = MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
    configuration.setString("rest.flamegraph.enabled", "true");
    env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration)
            .enableCheckpointing(100)
            .setParallelism(PARALLELISM)
            .setMaxParallelism(PARALLELISM);
    env.getConfig().enableObjectReuse();
  }

  @AfterEach
  void after() {
    for (TableIdentifier identifier : IDENTIFIERS) {
      CATALOG_EXTENSION.catalog().dropTable(identifier);
    }
  }

  private static class IdBasedGenerator implements DynamicRecordGenerator<Integer> {

    @Override
    public void generate(Integer id, Collector<DynamicRecord> out) {
      out.collect(rows.get(id % SAMPLE_SIZE));
    }
  }

  @Test
  void testDynamicSink() throws Exception {
    // So we make sure that the writer threads are the same for the 2 tests
    env.setMaxParallelism(PARALLELISM * TABLE_NUM * 2);
    env.setParallelism(PARALLELISM * TABLE_NUM * 2);
    runTest(
        s -> {
          DynamicIcebergSink.forInput(s)
              .generator(new IdBasedGenerator())
              .immediateTableUpdate(true)
              .catalogLoader(CATALOG_EXTENSION.catalogLoader())
              .append();
        });
  }

  @Test
  void testIcebergSink() throws Exception {
    runTest(
        s -> {
          for (int i = 0; i < IDENTIFIERS.length; ++i) {
            TableLoader tableLoader =
                TableLoader.fromCatalog(CATALOG_EXTENSION.catalogLoader(), IDENTIFIERS[i]);
            final int finalInt = i;
            IcebergSink.forRowData(
                    s.flatMap(
                            (FlatMapFunction<Integer, RowData>)
                                (input, collector) -> {
                                  if (input % TABLE_NUM == finalInt) {
                                    collector.collect(rows.get(input % SAMPLE_SIZE).rowData());
                                  }
                                })
                        .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(SCHEMA)))
                        .rebalance())
                .tableLoader(tableLoader)
                .uidSuffix("Uid" + i)
                .writeParallelism(WRITE_PARALLELISM)
                .append();
          }
        });
  }

  private void runTest(Consumer<DataStream<Integer>> sink) throws Exception {
    DataStream<Integer> dataStream =
        env.addSource(
            new BoundedTestSource<>(
                ImmutableList.of(
                    RANGE, RANGE, RANGE, RANGE, RANGE, RANGE, RANGE, RANGE, RANGE, RANGE),
                true),
            TypeInformation.of(Integer.class));

    sink.accept(dataStream);

    long before = System.currentTimeMillis();
    env.execute();

    for (TableIdentifier identifier : IDENTIFIERS) {
      Table table = CATALOG_EXTENSION.catalog().loadTable(identifier);
      for (Snapshot snapshot : table.snapshots()) {
        long records = 0;
        for (DataFile dataFile : snapshot.addedDataFiles(table.io())) {
          records += dataFile.recordCount();
        }

        LOG.info(
            "TEST RESULT: For table {} snapshot {} written {} records in {} ms",
            identifier,
            snapshot.snapshotId(),
            records,
            snapshot.timestampMillis() - before);
        before = snapshot.timestampMillis();
      }
    }
  }
}
