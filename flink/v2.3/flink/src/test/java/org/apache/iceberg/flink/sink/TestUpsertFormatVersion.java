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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestUpsertFormatVersion extends TestFlinkIcebergSinkBase {

  @ParameterizedTest(name = "useV2Sink = {0}")
  @ValueSource(booleans = {true, false})
  void upsertFailsOnV4Table(boolean useV2Sink) {
    Table table = createTable(4);

    assertThatThrownBy(() -> appendUpsertSink(table, useV2Sink))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot write to table %s in UPSERT mode: format version 4 does not allow equality deletes",
            table.name());
  }

  @ParameterizedTest(name = "useV2Sink = {0}")
  @ValueSource(booleans = {true, false})
  void upsertIsAllowedOnV3Table(boolean useV2Sink) {
    Table table = createTable(3);

    assertThatCode(() -> appendUpsertSink(table, useV2Sink)).doesNotThrowAnyException();
  }

  private static Table createTable(int formatVersion) {
    return CATALOG_EXTENSION
        .catalog()
        .createTable(
            TestFixtures.TABLE_IDENTIFIER,
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
  }

  private static void appendUpsertSink(Table table, boolean useV2Sink) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(ImmutableList.of()), ROW_TYPE_INFO);
    if (useV2Sink) {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(CATALOG_EXTENSION.tableLoader())
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .append();
    } else {
      FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(CATALOG_EXTENSION.tableLoader())
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .append();
    }
  }
}
