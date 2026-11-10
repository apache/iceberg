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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDynamicTableUpdateOperator {

  private static final boolean CASE_SENSITIVE = true;
  private static final boolean CASE_INSENSITIVE = false;

  private static final boolean DROP_COLUMNS = true;
  private static final boolean PRESERVE_COLUMNS = false;

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TABLE);

  private static final Schema SCHEMA1 =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final Schema REQUIRED_INT_ID =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final Schema REQUIRED_LONG_ID =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final OutputTag<Tuple2<DynamicRecordInternal, Exception>> ERROR_STREAM =
      new OutputTag<>(
          DynamicRecordProcessor.ERROR_STREAM,
          TypeInformation.of(new TypeHint<Tuple2<DynamicRecordInternal, Exception>>() {}));

  private static final Schema SCHEMA2 =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  void testDynamicTableUpdateOperatorNewTable() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    assertThat(catalog.tableExists(table)).isFalse();
    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            null);
    operator.open(null);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA1,
            GenericRowData.of(1, "test"),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = processRecord(operator, input);

    assertThat(catalog.tableExists(table)).isTrue();
    assertThat(input).isEqualTo(output);
  }

  @Test
  void testDynamicTableUpdateOperatorSchemaChange() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            null);
    operator.open(null);

    catalog.createTable(table, SCHEMA1);
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA2,
            GenericRowData.of(1, "test"),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = processRecord(operator, input);

    assertThat(catalog.loadTable(table).schema().sameSchema(SCHEMA2)).isTrue();
    assertThat(input).isEqualTo(output);

    // Process the same input again
    DynamicRecordInternal output2 = processRecord(operator, input);
    assertThat(output2).isEqualTo(output);
    assertThat(catalog.loadTable(table).schema().schemaId()).isEqualTo(output.schema().schemaId());
  }

  @Test
  void testEqualityFieldIdsRemappedToTableSchema() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            null);
    operator.open(null);

    catalog.createTable(table, SCHEMA1);
    Schema inputSchema =
        new Schema(
            Types.NestedField.required(2, "id", Types.IntegerType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));
    Types.NestedField equalityField = inputSchema.findField("id");
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            inputSchema,
            GenericRowData.of(1, StringData.fromString("test")),
            PartitionSpec.unpartitioned(),
            42,
            true,
            Collections.singleton(equalityField.fieldId()));
    DynamicRecordInternal output = processRecord(operator, input);

    assertThat(output.schema().findColumnName(equalityField.fieldId()))
        .isNotEqualTo(equalityField.name());
    assertThat(output.equalityFields())
        .map(output.schema()::findColumnName)
        .containsExactly(equalityField.name());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCaseInSensitivity(boolean caseSensitive) throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    Schema initialSchema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Schema caseSensitiveSchema =
        new Schema(Types.NestedField.required(1, "Id", Types.IntegerType.get()));

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(caseSensitive, PRESERVE_COLUMNS),
            null);
    operator.open(null);

    catalog.createTable(table, initialSchema);
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            caseSensitiveSchema,
            GenericRowData.of(1, "test"),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = processRecord(operator, input);

    if (caseSensitive) {
      // Schema changes due to case sensitivity
      Schema expectedSchema =
          new Schema(
              Types.NestedField.optional(2, "Id", Types.IntegerType.get()),
              Types.NestedField.optional(1, "id", Types.IntegerType.get()));
      Schema tableSchema = catalog.loadTable(table).schema();
      assertThat(tableSchema.sameSchema(expectedSchema)).isTrue();
      assertThat(output.schema().sameSchema(expectedSchema)).isTrue();
    } else {
      // No schema change due to case insensitivity
      assertThat(catalog.loadTable(table).schema().sameSchema(initialSchema)).isTrue();
      assertThat(output.schema().sameSchema(initialSchema)).isTrue();
    }
  }

  @Test
  void testDynamicTableUpdateOperatorPreserveUnusedColumns() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            null);
    operator.open(null);

    catalog.createTable(table, SCHEMA2);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA1,
            GenericRowData.of(1),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = processRecord(operator, input);

    Schema tableSchema = catalog.loadTable(table).schema();
    assertThat(tableSchema.columns()).hasSize(2);
    assertThat(tableSchema.findField("id")).isNotNull();
    assertThat(tableSchema.findField("data")).isNotNull();
    assertThat(tableSchema.findField("data").isOptional()).isTrue();
    assertThat(input).isEqualTo(output);
  }

  @Test
  void testDynamicTableUpdateOperatorDropUnusedColumns() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_INSENSITIVE, DROP_COLUMNS),
            null);
    operator.open(null);

    catalog.createTable(table, SCHEMA2);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA1,
            GenericRowData.of(1),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = processRecord(operator, input);

    Schema tableSchema = catalog.loadTable(table).schema();
    assertThat(tableSchema.columns()).hasSize(1);
    assertThat(tableSchema.findField("id")).isNotNull();
    assertThat(tableSchema.findField("data")).isNull();
    assertThat(input).isEqualTo(output);
  }

  @Test
  void errorStreamReceivesRecordWhoseUpdateFails() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), SCHEMA1);
    // int -> boolean is not a valid schema change
    Schema incompatibleSchema =
        new Schema(Types.NestedField.required(1, "id", Types.BooleanType.get()));
    DynamicRecordInternal input = record(incompatibleSchema, GenericRowData.of(true));

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM)).hasSize(1);
      Tuple2<DynamicRecordInternal, Exception> diverted =
          harness.getSideOutput(ERROR_STREAM).poll().getValue();
      assertThat(diverted.f0).isEqualTo(input);
      assertThat(diverted.f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessage("Cannot change column type: id: int -> boolean");
    }
  }

  @Test
  void failureTheFilterRejectsPropagates() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), SCHEMA1);
    Schema incompatibleSchema =
        new Schema(Types.NestedField.required(1, "id", Types.BooleanType.get()));

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> false)) {
      assertThatThrownBy(
              () -> harness.processElement(record(incompatibleSchema, GenericRowData.of(true)), 0L))
          .isInstanceOf(DynamicRecordException.class)
          .hasMessageContaining("Cannot change column type: id: int -> boolean");
      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
    }
  }

  @Test
  void divertedRecordIsTheUnmodifiedInput() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), REQUIRED_LONG_ID);
    // the id has to be converted to long and the data is missing for a required column
    DynamicRecordInternal input = record(REQUIRED_INT_ID, GenericRowData.of(1, null));

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

      DynamicRecordInternal diverted = harness.getSideOutput(ERROR_STREAM).poll().getValue().f0;
      assertThat(diverted.schema().asStruct()).isEqualTo(REQUIRED_INT_ID.asStruct());
      assertThat(diverted.rowData().getInt(0)).isEqualTo(1);
    }
  }

  @Test
  void failureAfterTheTableWasUpdatedIsDiverted() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), SCHEMA1);
    // the record adds a column to the table and then fails the upsert validation
    DynamicRecordInternal input =
        upsertRecord(SCHEMA2, GenericRowData.of(1, StringData.fromString("a")), Set.of());

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM).poll().getValue().f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessageContaining("Equality field columns shouldn't be empty");
      assertThat(
              CATALOG_EXTENSION
                  .catalog()
                  .loadTable(TableIdentifier.of(TABLE))
                  .schema()
                  .findField("data"))
          .isNotNull();
    }
  }

  @Test
  void upsertFallsBackToIdentifierFieldsOfTable() throws Exception {
    CATALOG_EXTENSION
        .catalog()
        .createTable(
            TableIdentifier.of(TABLE),
            new Schema(
                java.util.List.of(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get())),
                Set.of(1)));
    // the schema of the record has no identifier fields and the record no equality fields
    DynamicRecordInternal input =
        upsertRecord(SCHEMA2, GenericRowData.of(1, StringData.fromString("a")), Set.of());

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void upsertOnOlderUnpartitionedSpecOfPartitionedTable() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), SCHEMA2);
    PartitionSpec olderSpec = table.spec();
    table.updateSpec().addField("data").commit();

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA2,
            GenericRowData.of(1, StringData.fromString("a")),
            olderSpec,
            42,
            true,
            Set.of(1));

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

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
                TableIdentifier.of(TABLE),
                SCHEMA2,
                PartitionSpec.builderFor(SCHEMA2).identity("data").build());
    PartitionSpec olderSpec = table.spec();
    table.updateSpec().removeField("data").commit();

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA2,
            GenericRowData.of(1, StringData.fromString("a")),
            olderSpec,
            42,
            true,
            Set.of(1));

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM).poll().getValue().f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessageContaining("should be included in equality fields");
    }
  }

  @Test
  void nullInRequiredColumnOfRecordWithSchemaOfTableIsDiverted() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), REQUIRED_INT_ID);

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(record(REQUIRED_INT_ID, GenericRowData.of(1, null)), 0L);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(ERROR_STREAM).poll().getValue().f1)
          .isInstanceOf(DynamicRecordException.class)
          .hasMessage("Null value for non-nullable field: data");
    }
  }

  @Test
  void upsertDeleteWithoutRequiredColumnsIsNotDiverted() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of(TABLE), REQUIRED_LONG_ID);
    DynamicRecordInternal input =
        upsertRecord(REQUIRED_INT_ID, GenericRowData.ofKind(RowKind.DELETE, 1, null), Set.of(1));

    try (OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal> harness =
        harness(error -> true)) {
      harness.processElement(input, 0L);

      assertThat(harness.getSideOutput(ERROR_STREAM)).isNull();
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  private static OneInputStreamOperatorTestHarness<DynamicRecordInternal, DynamicRecordInternal>
      harness(DynamicRecordExceptionFilter filter) throws Exception {
    return ProcessFunctionTestHarnesses.forProcessFunction(
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            filter));
  }

  private static DynamicRecordInternal record(Schema schema, RowData row) {
    return new DynamicRecordInternal(
        TABLE, "branch", schema, row, PartitionSpec.unpartitioned(), 42, false, Set.of());
  }

  private static DynamicRecordInternal upsertRecord(
      Schema schema, RowData row, Set<Integer> equalityFieldIds) {
    return new DynamicRecordInternal(
        TABLE, "branch", schema, row, PartitionSpec.unpartitioned(), 42, true, equalityFieldIds);
  }

  private DynamicRecordInternal processRecord(
      DynamicTableUpdateOperator operator, DynamicRecordInternal input) throws Exception {
    TestCollector collector = new TestCollector();
    operator.processElement(input, null, collector);
    return collector.getResult();
  }

  private static class TestCollector implements Collector<DynamicRecordInternal> {
    private DynamicRecordInternal result;

    @Override
    public void collect(DynamicRecordInternal record) {
      this.result = record;
    }

    @Override
    public void close() {}

    DynamicRecordInternal getResult() {
      return result;
    }
  }

  private static FlinkDynamicSinkConf flinkDynamicSinkConfiguration(
      boolean caseSensitive, boolean dropUnusedColumns) {
    return new FlinkDynamicSinkConf(
        ImmutableMap.of(
            FlinkDynamicSinkOptions.CASE_SENSITIVE.key(), String.valueOf(caseSensitive),
            FlinkDynamicSinkOptions.DROP_UNUSED_COLUMNS.key(), String.valueOf(dropUnusedColumns)),
        new Configuration());
  }
}
