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

import static org.apache.iceberg.TestHelpers.waitUntilAfter;
import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.flink.TestFixtures.TABLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestDynamicRecordProcessor {

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TABLE);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final Schema SCHEMA_WITH_IDENTIFIER =
      new Schema(
          Lists.newArrayList(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get())),
          Sets.newHashSet(1));

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("default", "table");
  private static final String BRANCH = SnapshotRef.MAIN_BRANCH;
  private static final int CACHE_REFRESH_MS = 10;

  @Test
  void testForwardEligibleWhenNoEqualityFields() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, null);

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isTrue();
  }

  @Test
  void testNotForwardEligibleWhenDistributionModeSet() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, DistributionMode.NONE);

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void testNotForwardEligibleWhenUserEqualityFieldsSet() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, null);
    record.setEqualityFields(Collections.singleton("id"));

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void testNotForwardEligibleWhenSchemaHasIdentifierFields() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA_WITH_IDENTIFIER, null);

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void testNotForwardEligibleWhenEmptyEqualityFieldsButIdentifierFieldsPresent() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA_WITH_IDENTIFIER, null);
    record.setEqualityFields(Collections.emptySet());

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void routesRecordsToMainOutputAfterTableCreationAndCacheRefresh() throws Exception {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, DistributionMode.NONE);

    OutputTag<DynamicRecordInternal> updateStream = updateStream();
    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processorWithoutImmediateUpdate())) {
      harness.processElement(record, 0L);
      assertThat(harness.getSideOutput(updateStream)).hasSize(1);
      assertThat(harness.extractOutputValues()).isEmpty();

      Table table = CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
      table.manageSnapshots().createBranch(BRANCH).commit();
      waitUntilAfter(System.currentTimeMillis() + CACHE_REFRESH_MS);

      harness.processElement(record, 1L);
      assertThat(harness.getSideOutput(updateStream)).hasSize(1);
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void routesRecordsToMainOutputAfterSchemaUpdateAndCacheRefresh() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    table.newAppend().commit();
    Schema schemaWithNewColumn =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));
    DynamicRecord record =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            BRANCH,
            schemaWithNewColumn,
            GenericRowData.of(1, StringData.fromString("foo"), StringData.fromString("bar")),
            PartitionSpec.unpartitioned(),
            DistributionMode.NONE,
            2);

    OutputTag<DynamicRecordInternal> updateStream = updateStream();
    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processorWithoutImmediateUpdate())) {
      harness.processElement(record, 0L);
      assertThat(harness.getSideOutput(updateStream)).hasSize(1);
      assertThat(harness.extractOutputValues()).isEmpty();

      table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
      waitUntilAfter(System.currentTimeMillis() + CACHE_REFRESH_MS);

      harness.processElement(record, 1L);
      assertThat(harness.getSideOutput(updateStream)).hasSize(1);
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  private static DynamicRecordProcessor<DynamicRecord> processorWithoutImmediateUpdate() {
    return new DynamicRecordProcessor<>(
        (input, out) -> out.collect(input),
        CATALOG_EXTENSION.catalogLoader(),
        TableCreator.DEFAULT,
        new FlinkDynamicSinkConf(
            ImmutableMap.of(
                FlinkDynamicSinkOptions.IMMEDIATE_TABLE_UPDATE.key(),
                "false",
                FlinkDynamicSinkOptions.CACHE_REFRESH_MS.key(),
                String.valueOf(CACHE_REFRESH_MS)),
            new Configuration()),
        Collections.emptyMap(),
        new Configuration());
  }

  private static OutputTag<DynamicRecordInternal> updateStream() {
    return new OutputTag<>(
        DynamicRecordProcessor.DYNAMIC_TABLE_UPDATE_STREAM,
        new DynamicRecordInternalType(
            CATALOG_EXTENSION.catalogLoader(), true, CACHE_REFRESH_MS)) {};
  }

  private static DynamicRecord recordWithDistributionMode(
      Schema schema, DistributionMode distributionMode) {
    return new DynamicRecord(
        TABLE_IDENTIFIER,
        BRANCH,
        schema,
        GenericRowData.of(1, StringData.fromString("foo")),
        PartitionSpec.unpartitioned(),
        distributionMode,
        2);
  }
}
