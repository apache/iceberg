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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
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

  private static final Schema SCHEMA_LONG_ID =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final OutputTag<Tuple2<DynamicRecordInternal, Exception>> ERROR_STREAM =
      new OutputTag<>(
          DynamicRecordProcessor.ERROR_STREAM,
          TypeInformation.of(new TypeHint<Tuple2<DynamicRecordInternal, Exception>>() {}));

  private static final OutputTag<DynamicRecordInternal> FORWARD_STREAM =
      new OutputTag<>(
          DynamicRecordProcessor.DYNAMIC_FORWARD_STREAM,
          TypeInformation.of(DynamicRecordInternal.class));

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

  @Test
  void divertsRecordThatCannotBeConverted() throws Exception {
    // the table has a long id, so the id of the record is converted and the data must be present
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA_LONG_ID);
    DynamicRecord record = record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, null));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM)).hasSize(1);
      assertThat(harness.getSideOutput(ERROR_STREAM).poll().getValue().f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessage("Null value for non-nullable field: data");
    }
  }

  @Test
  void divertsRecordWithNullInRequiredColumnWhenSchemaMatchesTable() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecord record = record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, null));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void divertsForwardRecordWithNullInRequiredColumn() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecord record = record(SCHEMA, null, GenericRowData.of(1, null));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.getSideOutput(FORWARD_STREAM)).isNull();
      assertThat(harness.getSideOutput(ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void routesValidForwardRecordToForwardOutput() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecord record = record(SCHEMA, null, GenericRowData.of(1, StringData.fromString("a")));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.getSideOutput(FORWARD_STREAM)).hasSize(1);
      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
    }
  }

  @Test
  void divertsRecordWithEqualityFieldMissingFromSchema() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecord record =
        record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, StringData.fromString("a")));
    record.setUpsertMode(true);
    record.setEqualityFields(Collections.singleton("missing"));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.getSideOutput(ERROR_STREAM).poll().getValue().f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessage("Equality field missing does not exist in schema");
    }
  }

  @Test
  void doesNotDivertRowThatOnlyDeletes() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA_LONG_ID);
    DynamicRecord record =
        record(SCHEMA, DistributionMode.NONE, GenericRowData.ofKind(RowKind.DELETE, 1, null));
    record.setUpsertMode(true);
    record.setEqualityFields(Collections.singleton("id"));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void upsertOnOlderUnpartitionedSpecOfPartitionedTable() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    PartitionSpec olderSpec = table.spec();
    table.updateSpec().addField("data").commit();
    DynamicRecord record =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            BRANCH,
            SCHEMA,
            GenericRowData.of(1, StringData.fromString("a")),
            olderSpec,
            DistributionMode.NONE,
            1);
    record.setUpsertMode(true);
    record.setEqualityFields(Collections.singleton("id"));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void upsertOnOlderPartitionedSpecOfUnpartitionedTable() throws Exception {
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TABLE_IDENTIFIER,
                SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("data").build());
    PartitionSpec olderSpec = table.spec();
    table.updateSpec().removeField("data").commit();
    DynamicRecord record =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            BRANCH,
            SCHEMA,
            GenericRowData.of(1, StringData.fromString("a")),
            olderSpec,
            DistributionMode.NONE,
            1);
    record.setUpsertMode(true);
    record.setEqualityFields(Collections.singleton("id"));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> true))) {
      harness.processElement(record, 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM).poll().getValue().f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessageContaining("should be included in equality fields");
    }
  }

  @Test
  void propagatesFailureRejectedByFilter() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecord record = record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, null));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(error -> false))) {
      assertThatThrownBy(() -> harness.processElement(record, 0L))
          .isInstanceOf(DynamicRecordException.class)
          .hasMessage("Null value for non-nullable field: data");
      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
    }
  }

  @Test
  void propagatesFailureWithoutErrorStream() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA_LONG_ID);
    DynamicRecord record = record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, null));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor(null))) {
      assertThatThrownBy(() -> harness.processElement(record, 0L))
          .isInstanceOf(DynamicRecordException.class)
          .hasMessage("Null value for non-nullable field: data");
    }
  }

  @Test
  void doesNotDivertFailureOfGenerator() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecordProcessor<DynamicRecord> processor =
        processor(
            (input, out) -> {
              throw new IllegalStateException("generator failed");
            },
            error -> true);
    DynamicRecord record =
        record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, StringData.fromString("a")));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor)) {
      assertThatThrownBy(() -> harness.processElement(record, 0L))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("generator failed");
      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
    }
  }

  @Test
  void doesNotDivertFailureOfOperatorChainedToForwardOutput() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecordProcessor<DynamicRecord> processor = processor(error -> true);
    DynamicRecord record = record(SCHEMA, null, GenericRowData.of(1, StringData.fromString("a")));

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor)) {
      List<String> outputs = Lists.newArrayList();
      ProcessFunction<DynamicRecord, DynamicRecordInternal>.Context failingContext =
          processor.new Context() {
            @Override
            public Long timestamp() {
              return null;
            }

            @Override
            public TimerService timerService() {
              throw new UnsupportedOperationException();
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
              outputs.add(outputTag.getId());
              if (DynamicRecordProcessor.DYNAMIC_FORWARD_STREAM.equals(outputTag.getId())) {
                throw new IllegalStateException("writer chained to the forward output failed");
              }
            }
          };

      assertThatThrownBy(() -> processor.processElement(record, failingContext, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("writer chained to the forward output failed");
      assertThat(outputs).doesNotContain(DynamicRecordProcessor.ERROR_STREAM);
    }
  }

  @Test
  void doesNotDivertFailureOfOperatorChainedToMainOutput() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    DynamicRecordProcessor<DynamicRecord> processor = processor(error -> true);
    DynamicRecord record =
        record(SCHEMA, DistributionMode.NONE, GenericRowData.of(1, StringData.fromString("a")));
    Collector<DynamicRecordInternal> failingCollector =
        new Collector<>() {
          @Override
          public void collect(DynamicRecordInternal value) {
            throw new IllegalStateException("operator chained to the main output failed");
          }

          @Override
          public void close() {}
        };

    try (OneInputStreamOperatorTestHarness<DynamicRecord, DynamicRecordInternal> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(processor)) {
      ProcessFunction<DynamicRecord, DynamicRecordInternal>.Context context =
          processor.new Context() {
            @Override
            public Long timestamp() {
              return null;
            }

            @Override
            public TimerService timerService() {
              throw new UnsupportedOperationException();
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
              throw new AssertionError("nothing is expected on side output " + outputTag.getId());
            }
          };

      assertThatThrownBy(() -> processor.processElement(record, context, failingCollector))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("operator chained to the main output failed");
    }
  }

  private static DynamicRecord record(Schema schema, DistributionMode mode, RowData row) {
    return mode == null
        ? new DynamicRecord(TABLE_IDENTIFIER, BRANCH, schema, row, PartitionSpec.unpartitioned())
        : new DynamicRecord(
            TABLE_IDENTIFIER, BRANCH, schema, row, PartitionSpec.unpartitioned(), mode, 1);
  }

  private static DynamicRecordProcessor<DynamicRecord> processor(
      DynamicRecordExceptionFilter errorFilter) {
    return processor((input, out) -> out.collect(input), errorFilter);
  }

  private static DynamicRecordProcessor<DynamicRecord> processor(
      DynamicRecordGenerator<DynamicRecord> generator, DynamicRecordExceptionFilter errorFilter) {
    return new DynamicRecordProcessor<>(
        generator,
        CATALOG_EXTENSION.catalogLoader(),
        TableCreator.DEFAULT,
        new FlinkDynamicSinkConf(
            ImmutableMap.of(FlinkDynamicSinkOptions.IMMEDIATE_TABLE_UPDATE.key(), "true"),
            new Configuration()),
        Collections.emptyMap(),
        new Configuration(),
        errorFilter);
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
        new Configuration(),
        null);
  }

  private static OutputTag<DynamicRecordInternal> updateStream() {
    return new OutputTag<>(
        DynamicRecordProcessor.DYNAMIC_TABLE_UPDATE_STREAM,
        new DynamicRecordInternalType(
            CATALOG_EXTENSION.catalogLoader(), true, CACHE_REFRESH_MS, 10)) {};
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
