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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDynamicRecordWithConfig {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "table");
  private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();
  private static final RowData ROW_DATA = GenericRowData.of(1, StringData.fromString("test"));

  @Test
  void testBranchFallBack() {
    String defaultBranch = "default-branch";
    FlinkWriteConf conf =
        new FlinkWriteConf(
            ImmutableMap.of(FlinkWriteOptions.BRANCH.key(), defaultBranch), new Configuration());
    DynamicRecordWithConfig dynamicRecordWithConfig = new DynamicRecordWithConfig(conf);

    DynamicRecord dynamicRecord =
        new DynamicRecord(TABLE_IDENTIFIER, null, SCHEMA, ROW_DATA, UNPARTITIONED);
    assertThat(dynamicRecordWithConfig.wrap(dynamicRecord).branch()).isEqualTo(defaultBranch);

    String customBranch = "custom-branch";
    dynamicRecord.setBranch(customBranch);
    assertThat(dynamicRecordWithConfig.wrap(dynamicRecord).branch()).isEqualTo(customBranch);
  }

  @Test
  void testWriteParallelismFallBack() {
    int defaultParallelism = 4;
    FlinkWriteConf conf =
        new FlinkWriteConf(
            ImmutableMap.of(
                FlinkWriteOptions.WRITE_PARALLELISM.key(), String.valueOf(defaultParallelism)),
            new Configuration());
    DynamicRecordWithConfig dynamicRecordWithConfig = new DynamicRecordWithConfig(conf);

    DynamicRecord dynamicRecord =
        new DynamicRecord(TABLE_IDENTIFIER, null, SCHEMA, ROW_DATA, UNPARTITIONED, null, -1);
    assertThat(dynamicRecordWithConfig.wrap(dynamicRecord).writeParallelism())
        .isEqualTo(defaultParallelism);

    dynamicRecord.writeParallelism(0);
    assertThat(dynamicRecordWithConfig.wrap(dynamicRecord).writeParallelism())
        .isEqualTo(defaultParallelism);

    dynamicRecord.writeParallelism(8);
    assertThat(dynamicRecordWithConfig.wrap(dynamicRecord).writeParallelism()).isEqualTo(8);
  }

  @Test
  void testDelegatesToWrappedRecord() {
    FlinkWriteConf conf = new FlinkWriteConf(Collections.emptyMap(), new Configuration());
    PartitionSpec partitioned = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Set<String> equalityFields = ImmutableSet.of("id", "data");

    DynamicRecord dynamicRecord =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            SnapshotRef.MAIN_BRANCH,
            SCHEMA,
            ROW_DATA,
            partitioned,
            DistributionMode.HASH,
            2);
    dynamicRecord.setUpsertMode(true);
    dynamicRecord.setEqualityFields(equalityFields);

    DynamicRecordWithConfig record = new DynamicRecordWithConfig(conf).wrap(dynamicRecord);

    assertThat(record.tableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
    assertThat(record.schema()).isEqualTo(SCHEMA);
    assertThat(record.spec()).isEqualTo(partitioned);
    assertThat(record.rowData()).isSameAs(ROW_DATA);
    assertThat(record.distributionMode()).isEqualTo(DistributionMode.HASH);
    assertThat(record.upsertMode()).isTrue();
    assertThat(record.equalityFields()).isEqualTo(equalityFields);
  }
}
