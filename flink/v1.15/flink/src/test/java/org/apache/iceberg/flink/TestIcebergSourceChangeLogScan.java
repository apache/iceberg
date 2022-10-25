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
package org.apache.iceberg.flink;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceChangeLogScan {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopTableResource tableResource =
      new HadoopTableResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, SCHEMA);

  public static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "dt", Types.StringType.get()));

  @Test
  public void testChangeLogStreamingTable() throws Exception {
    // snapshot1
    Row row1 = Row.ofKind(RowKind.INSERT, 1, "aaa", "2021-01-01");
    DataFile dataFile = writeFile("2021-01-01", tableResource.table(), row1);
    tableResource.table().newAppend().appendFile(dataFile).commit();

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .scanMode("CHANGELOG_SCAN")
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 1);
      TestHelpers.assertRows(result1, ImmutableList.of(row1));

      // snapshot2
      Row row2 = Row.ofKind(RowKind.INSERT, 2, "bbb", "2021-01-02");
      DataFile dataFile2 = writeFile("2021-01-02", tableResource.table(), row2);
      tableResource.table().newAppend().appendFile(dataFile2).commit();

      List<Row> result2 = waitForResult(iter, 1);
      TestHelpers.assertRows(result2, ImmutableList.of(row2));

      // snapshot3
      tableResource.table().newDelete().deleteFile(dataFile).commit();

      List<Row> result3 = waitForResult(iter, 1);
      Row row1Delete = Row.ofKind(RowKind.DELETE, 1, "aaa", "2021-01-01");
      TestHelpers.assertRows(result3, ImmutableList.of(row1Delete));
    }
  }

  private DataFile writeFile(String partition, Table table, Row... rows) throws IOException {
    GenericAppenderHelper appender =
        new GenericAppenderHelper(table, FileFormat.AVRO, TEMPORARY_FOLDER);

    GenericRecord gRecord = GenericRecord.create(table.schema());
    List<Record> records = Lists.newArrayList();
    for (Row row : rows) {
      records.add(
          gRecord.copy(
              "id", row.getField(0),
              "data", row.getField(1),
              "dt", row.getField(2)));
    }

    return appender.writeFile(org.apache.iceberg.TestHelpers.Row.of(partition, 0), records);
  }

  private DataStream<Row> createStream(ScanContext scanContext) throws Exception {
    // start the source and collect output
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Row> stream =
        env.fromSource(
                IcebergSource.forRowData()
                    .tableLoader(tableResource.tableLoader())
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(scanContext.isStreaming())
                    .streamingStartingStrategy(scanContext.streamingStartingStrategy())
                    .startSnapshotTimestamp(scanContext.startSnapshotTimestamp())
                    .startSnapshotId(scanContext.startSnapshotId())
                    .monitorInterval(Duration.ofMillis(100L))
                    .scanMode(scanContext.scanMode())
                    .build(),
                WatermarkStrategy.noWatermarks(),
                "icebergSource",
                TypeInformation.of(RowData.class))
            .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(tableResource.table().schema())));
    return stream;
  }

  public static List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    List<Row> results = Lists.newArrayListWithCapacity(limit);
    while (results.size() < limit) {
      if (iter.hasNext()) {
        results.add(iter.next());
      } else {
        break;
      }
    }
    return results;
  }
}
