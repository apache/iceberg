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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorConfig;
import org.apache.iceberg.flink.source.reader.FlinkBulkFormatAdaptor;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestIcebergSourceContinuous extends AbstractTestBase {

  private HadoopCatalog catalog;
  private String warehouse;
  private String location;
  private TableLoader tableLoader;

  private final FileFormat fileFormat = FileFormat.PARQUET;

  private Table table;
  private GenericAppenderHelper dataAppender;

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);
    location = String.format("%s/%s/%s", warehouse, TestFixtures.DATABASE, TestFixtures.TABLE);
    tableLoader = TableLoader.fromHadoopTable(location);

    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    dataAppender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
  }

  @After
  public void after() throws IOException {
    catalog.close();
    tableLoader.close();
  }

  // need latest change in DataStreamUtils
  @Test
  public void testTableScanThenIncremental() throws Exception {

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(table.schema(), 2, 0L);
    dataAppender.appendToTable(batch1);
    final long snapshotId1 = table.currentSnapshot().snapshotId();

    final ScanContext scanContext = ScanContext.builder()
        .project(table.schema())
        .build();
    final RowType rowType = FlinkSchemaUtil.convert(scanContext.project());

    // start the source and collect output
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    final DataStream<Row> stream = env.fromSource(
        IcebergSource.<RowData>builder()
            .tableLoader(tableLoader)
            .assignerFactory(new SimpleSplitAssignerFactory())
            .bulkFormat(new FlinkBulkFormatAdaptor<>(ImmutableMap.of(
                FileFormat.PARQUET,
                new ParquetColumnarRowInputFormat<>(new Configuration(),
                    rowType, 128, false, true)
                )))
            .scanContext(scanContext)
            .enumeratorConfig(IcebergEnumeratorConfig.builder()
                .splitDiscoveryInterval(Duration.ofMillis(1000L))
                .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                .build())
            .build(),
        WatermarkStrategy.noWatermarks(),
        "icebergSource",
        TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(rowType));

    try (CloseableIterator<Row> iter = stream.executeAndCollect(getClass().getSimpleName())) {

      final List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, table.schema());

      // snapshot2
      final List<Record> batch2 = RandomGenericData.generate(table.schema(), 2, 1L);
      dataAppender.appendToTable(batch2);
      final long snapshotId2 = table.currentSnapshot().snapshotId();

      final List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, table.schema());

      // snapshot3
      final List<Record> batch3 = RandomGenericData.generate(table.schema(), 2, 2L);
      dataAppender.appendToTable(batch3);
      final long snapshotId3 = table.currentSnapshot().snapshotId();

      final List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, table.schema());
    }
  }

  private List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    List<Row> results = new ArrayList<>(limit);
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
