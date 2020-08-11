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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSink extends AbstractTestBase {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tablePath;
  private int parallelism;
  private boolean partitioned;

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tablePath = folder.getAbsolutePath();
  }

  @Parameterized.Parameters(name = "parallelism = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {1, true},
        new Object[] {1, false},
        new Object[] {2, true},
        new Object[] {2, false},
    };
  }

  public TestFlinkIcebergSink(int parallelism, boolean partitioned) {
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Test
  public void testDataStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    env.enableCheckpointing(100);
    env.setParallelism(parallelism);

    List<RowData> rows = Lists.newArrayList(
        SimpleDataUtil.createRowData(1, "hello"),
        SimpleDataUtil.createRowData(2, "world"),
        SimpleDataUtil.createRowData(3, "foo")
    );

    DataStream<RowData> dataStream = env.addSource(new FiniteTestSource<>(rows));

    Table table = SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), partitioned);
    Assert.assertNotNull(table);

    // Output the data stream to stdout.
    Map<String, String> options = ImmutableMap.of(
        "catalog-type", "hadoop",
        "warehouse", tablePath
    );

    IcebergSinkUtil.write(dataStream, parallelism, options, CONF, table, SimpleDataUtil.FLINK_SCHEMA);

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    List<RowData> expectedRows = Lists.newArrayList();
    expectedRows.addAll(rows);
    expectedRows.addAll(rows);

    SimpleDataUtil.assertTableRows(tablePath, expectedRows);
  }
}
