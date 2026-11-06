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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

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
  void testDynamicTableUpdateOperatorErrorStream() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    AtomicInteger errorCount = new AtomicInteger();
    AtomicReference<Tuple2<DynamicRecordInternal, Exception>> errorRecord = new AtomicReference<>();

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            error -> true);
    operator.open(null);

    catalog.createTable(table, SCHEMA1);

    // Incompatible evolution (int -> boolean) fails the update; the filter whitelists it, so the
    // record is routed to the side output instead of failing the job.
    Schema incompatibleSchema =
        new Schema(Types.NestedField.required(1, "id", Types.BooleanType.get()));
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            incompatibleSchema,
            GenericRowData.of(true),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());

    TestCollector collector = new TestCollector();
    ProcessFunction<DynamicRecordInternal, DynamicRecordInternal>.Context mockContext =
        Mockito.mock(ProcessFunction.Context.class);
    Mockito.doAnswer(
            invocation -> {
              errorCount.incrementAndGet();
              assertThat(((OutputTag) invocation.getArgument(0)).getId())
                  .isEqualTo(DynamicRecordProcessor.ERROR_STREAM);
              errorRecord.set(invocation.getArgument(1));
              return null;
            })
        .when(mockContext)
        .output(Mockito.any(OutputTag.class), Mockito.any(Tuple2.class));

    operator.processElement(input, mockContext, collector);

    assertThat(errorCount.get()).isEqualTo(1);
    assertThat(errorRecord.get().f0).isEqualTo(input);
    assertThat(errorRecord.get().f1.getMessage())
        .contains("Cannot change column type: id: int -> boolean");
    assertThat(collector.getResult()).isNull();
  }

  @Test
  void testDynamicTableUpdateOperatorPropagatesNonDivertableFailure() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    // Filter whitelists nothing, so even with the error stream wired the failure must propagate
    // rather than being diverted.
    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            TableCreator.DEFAULT,
            flinkDynamicSinkConfiguration(CASE_SENSITIVE, PRESERVE_COLUMNS),
            error -> false);
    operator.open(null);

    catalog.createTable(table, SCHEMA1);

    Schema incompatibleSchema =
        new Schema(Types.NestedField.required(1, "id", Types.BooleanType.get()));
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            incompatibleSchema,
            GenericRowData.of(true),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());

    TestCollector collector = new TestCollector();
    ProcessFunction<DynamicRecordInternal, DynamicRecordInternal>.Context mockContext =
        Mockito.mock(ProcessFunction.Context.class);

    assertThatThrownBy(() -> operator.processElement(input, mockContext, collector))
        .hasMessageContaining("Cannot change column type: id: int -> boolean");
    Mockito.verify(mockContext, Mockito.never())
        .output(Mockito.any(OutputTag.class), Mockito.any());
    assertThat(collector.getResult()).isNull();
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
