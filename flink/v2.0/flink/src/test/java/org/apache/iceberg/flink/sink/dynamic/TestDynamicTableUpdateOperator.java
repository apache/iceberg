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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class TestDynamicTableUpdateOperator {

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
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    assertThat(catalog.tableExists(table)).isFalse();
    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            false);
    operator.open((OpenContext) null);

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
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            false);
    operator.open((OpenContext) null);

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
  void testDynamicTableUpdateOperatorErrorStream() throws Exception {
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    AtomicInteger errorCount = new AtomicInteger();
    AtomicReference<Tuple2<DynamicRecordInternal, Exception>> errorRecord = new AtomicReference<>();

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            // Enable error stream
            true);
    operator.open((OpenContext) null);

    // Create table with integer id field
    catalog.createTable(table, SCHEMA1);

    // Try to process record with incompatible schema (int -> boolean)
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

    ProcessFunction.Context mockContext = Mockito.mock(ProcessFunction.Context.class);

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

    // Verify error
    assertThat(errorCount.get()).isEqualTo(1);
    assertThat(errorRecord.get().f0).isEqualTo(input);
    assertThat(errorRecord.get().f1.getMessage())
        .contains("Cannot change column type: id: int -> boolean");

    // Verify no output
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

    public DynamicRecordInternal getResult() {
      return result;
    }
  }
}
