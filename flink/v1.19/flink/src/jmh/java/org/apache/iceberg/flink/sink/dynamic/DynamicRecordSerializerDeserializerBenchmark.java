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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class DynamicRecordSerializerDeserializerBenchmark {
  private static final int SAMPLE_SIZE = 100_000;
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name2", Types.StringType.get()),
          Types.NestedField.required(3, "name3", Types.StringType.get()),
          Types.NestedField.required(4, "name4", Types.StringType.get()),
          Types.NestedField.required(5, "name5", Types.StringType.get()),
          Types.NestedField.required(6, "name6", Types.StringType.get()),
          Types.NestedField.required(7, "name7", Types.StringType.get()),
          Types.NestedField.required(8, "name8", Types.StringType.get()),
          Types.NestedField.required(9, "name9", Types.StringType.get()));

  private List<DynamicRecordInternal> rows = Lists.newArrayListWithExpectedSize(SAMPLE_SIZE);
  private DynamicRecordInternalType type;

  public static void main(String[] args) throws RunnerException {
    Options options =
        new OptionsBuilder()
            .include(DynamicRecordSerializerDeserializerBenchmark.class.getSimpleName())
            .build();
    new Runner(options).run();
  }

  @Setup
  public void setupBenchmark() throws IOException {
    List<Record> records = RandomGenericData.generate(SCHEMA, SAMPLE_SIZE, 1L);
    this.rows =
        records.stream()
            .map(
                r ->
                    new DynamicRecordInternal(
                        "t",
                        "main",
                        SCHEMA,
                        RowDataConverter.convert(SCHEMA, r),
                        PartitionSpec.unpartitioned(),
                        1,
                        false,
                        Collections.emptySet()))
            .collect(Collectors.toList());

    File warehouse = Files.createTempFile("perf-bench", null).toFile();
    CatalogLoader catalogLoader =
        CatalogLoader.hadoop(
            "hadoop",
            new Configuration(),
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getPath()));
    this.type = new DynamicRecordInternalType(catalogLoader, true, 100);
  }

  @Benchmark
  @Threads(1)
  public void testSerialize(Blackhole blackhole) throws IOException {
    TypeSerializer<DynamicRecordInternal> serializer =
        type.createSerializer((SerializerConfig) null);
    DataOutputSerializer outputView = new DataOutputSerializer(1024);
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      serializer.serialize(rows.get(i), outputView);
    }
  }

  @Benchmark
  @Threads(1)
  public void testSerializeAndDeserialize(Blackhole blackhole) throws IOException {
    TypeSerializer<DynamicRecordInternal> serializer =
        type.createSerializer((SerializerConfig) null);

    DataOutputSerializer outputView = new DataOutputSerializer(1024);
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      serializer.serialize(rows.get(i), outputView);
      serializer.deserialize(new DataInputDeserializer(outputView.getSharedBuffer()));
    }
  }
}
