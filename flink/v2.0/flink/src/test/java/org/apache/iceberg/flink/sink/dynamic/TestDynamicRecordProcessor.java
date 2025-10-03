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
import static org.apache.iceberg.flink.TestFixtures.TABLE_IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class TestDynamicRecordProcessor {

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TABLE);

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Test
  void testDynamicRecordProcessorErrorStream() throws Exception {
    TableIdentifier identifier = TableIdentifier.of(DATABASE, TABLE);
    CATALOG_EXTENSION.catalog().createTable(identifier, SCHEMA);

    AtomicInteger errorCount = new AtomicInteger();
    AtomicReference<Tuple2<DynamicRecordInternal, Exception>> errorRecord = new AtomicReference<>();

    // Create a generator that produces a DynamicRecord but causes processing error
    DynamicRecordGenerator<String> errorGenerator =
        (element, out) -> {
          DynamicRecord record =
              new DynamicRecord(
                  TABLE_IDENTIFIER,
                  "main",
                  // Try to process record with incompatible schema (int -> boolean)
                  new Schema(Types.NestedField.required(1, "id", Types.BooleanType.get())),
                  GenericRowData.of(42),
                  PartitionSpec.unpartitioned(),
                  DistributionMode.NONE,
                  10);
          out.collect(record);
        };

    DynamicRecordProcessor<String> processor =
        new DynamicRecordProcessor<>(
            errorGenerator,
            CATALOG_EXTENSION.catalogLoader(),
            true, // immediateUpdate
            10, // cacheMaximumSize
            1000, // cacheRefreshMs
            10, // inputSchemasPerTableCacheMaximumSize
            true // errorStreamEnabled
            );

    // Mock runtime context
    RuntimeContext mockRuntimeContext = Mockito.mock(RuntimeContext.class);
    TaskInfo mockTaskInfo = Mockito.mock(TaskInfo.class);
    Mockito.when(mockRuntimeContext.getTaskInfo()).thenReturn(mockTaskInfo);
    Mockito.when(mockTaskInfo.getMaxNumberOfParallelSubtasks()).thenReturn(1);

    processor.setRuntimeContext(mockRuntimeContext);
    processor.open((OpenContext) null);

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

    // Process an element that will cause a processing error
    processor.processElement("test-element", mockContext, collector);

    // Verify error was captured
    assertThat(errorCount.get()).isEqualTo(1);
    assertThat(errorRecord.get()).isNotNull();
    assertThat(errorRecord.get().f1).isInstanceOf(Exception.class);

    // Verify no normal output was produced
    assertThat(collector.getResult()).isNull();
  }

  private static class TestCollector implements Collector<DynamicRecordInternal> {
    private final java.util.List<DynamicRecordInternal> results = new java.util.ArrayList<>();

    @Override
    public void collect(DynamicRecordInternal record) {
      this.results.add(record);
    }

    public void close() {}

    public DynamicRecordInternal getResult() {
      return results.isEmpty() ? null : results.get(0);
    }

    public java.util.List<DynamicRecordInternal> getResults() {
      return results;
    }
  }
}
