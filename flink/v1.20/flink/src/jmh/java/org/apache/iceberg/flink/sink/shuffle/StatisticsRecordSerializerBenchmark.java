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
package org.apache.iceberg.flink.sink.shuffle;

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.twitter.chill.java.ArraysAsListSerializer;
import java.io.IOException;
import java.util.Random;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.FlinkChillPackageRegistrar;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * This benchmark in 1.20 only works with Java 11 probably due to usage of {@link
 * ArraysAsListSerializer} in {@link FlinkChillPackageRegistrar}. Flink 2.0 and above switched to
 * {@link DefaultSerializers#ArraysAsListSerializer} in Kryo 5.6.
 *
 * <p>Using Java 17 would result in the following error:
 *
 * <pre>
 *     java.lang.RuntimeException: java.lang.reflect.InaccessibleObjectException: Unable to make field private final java.lang.Object[] java.util.Arrays$ArrayList.a accessible: module java.base does not "opens java.util" to unnamed module @192b07fd
 *         at com.twitter.chill.java.ArraysAsListSerializer.<init>(ArraysAsListSerializer.java:69)
 *         at org.apache.flink.api.java.typeutils.runtime.kryo.FlinkChillPackageRegistrar.registerSerializers(FlinkChillPackageRegistrar.java:67)
 *         at org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer.getKryoInstance(KryoSerializer.java:504)
 *         at org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer.checkKryoInitialized(KryoSerializer.java:513)
 *         at org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer.serialize(KryoSerializer.java:339)
 *         at org.apache.iceberg.flink.sink.shuffle.StatisticsRecordSerializerBenchmark.testSerializer(StatisticsRecordSerializerBenchmark.java:128)
 * </pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class StatisticsRecordSerializerBenchmark {
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
          Types.NestedField.required(9, "name9", Types.StringType.get()),
          Types.NestedField.required(10, "name10", Types.StringType.get()));

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();
  private static final StatisticsOrRecordTypeInformation CUSTOM_TYPE_INFORMATION =
      new StatisticsOrRecordTypeInformation(FlinkSchemaUtil.convert(SCHEMA), SCHEMA, SORT_ORDER);
  private static final TypeSerializer<StatisticsOrRecord> CUSTOM_SERIALIZER =
      CUSTOM_TYPE_INFORMATION.createSerializer(new SerializerConfigImpl());
  private static final TypeInformation<StatisticsOrRecord> DEFAULT_TYPE_INFORMATION =
      TypeInformation.of(StatisticsOrRecord.class);
  private static final TypeSerializer<StatisticsOrRecord> DEFAULT_SERIALIZER =
      DEFAULT_TYPE_INFORMATION.createSerializer(new SerializerConfigImpl());

  private StatisticsOrRecord[] rows;

  @Setup
  public void setupBenchmark() {
    Random random = new Random();
    rows = new StatisticsOrRecord[SAMPLE_SIZE];
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      RowData rowData =
          GenericRowData.of(
              random.nextInt(),
              StringData.fromString(DataDistributionUtil.randomString("name2-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name3-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name4-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name5-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name6-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name7-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name8-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name9-", 200)),
              StringData.fromString(DataDistributionUtil.randomString("name10-", 200)));
      rows[i] = StatisticsOrRecord.fromRecord(rowData);
    }
  }

  @TearDown
  public void tearDownBenchmark() {}

  @Benchmark
  public void measureName(Blackhole bh) {}

  @Benchmark
  @Threads(1)
  public void testCustomSerializer(Blackhole blackhole) throws Exception {
    testSerializer(CUSTOM_SERIALIZER, blackhole);
  }

  @Benchmark
  @Threads(1)
  public void testDefaultSerializer(Blackhole blackhole) throws Exception {
    testSerializer(DEFAULT_SERIALIZER, blackhole);
  }

  private void testSerializer(TypeSerializer<StatisticsOrRecord> serializer, Blackhole blackhole)
      throws IOException {
    DataOutputSerializer outputView = new DataOutputSerializer(1024 * 4);
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      outputView.clear();
      serializer.serialize(rows[i], outputView);
      StatisticsOrRecord deserialized =
          serializer.deserialize(new DataInputDeserializer(outputView.getSharedBuffer()));
      blackhole.consume(deserialized);
    }
  }
}
