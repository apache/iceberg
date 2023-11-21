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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.LocationProvider;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class CharSequenceMapBenchmark {

  private static final Random RANDOM = ThreadLocalRandom.current();
  private static final Schema SCHEMA =
      new Schema(
          required(1, "col1", Types.IntegerType.get()),
          required(2, "col2", Types.IntegerType.get()),
          required(3, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("col1").identity("col2").build();
  private static final String TABLE_LOCATION =
      "s3:/secure-organization-bucket-name/database-name/specific_table_name/data";
  private static final Map<String, String> TABLE_PROPERTIES =
      ImmutableMap.of(TableProperties.OBJECT_STORE_ENABLED, "true");
  private static final LocationProvider LOCATIONS =
      LocationProviders.locationsFor(TABLE_LOCATION, TABLE_PROPERTIES);
  private static final int NUM_FILE_PATHS = 1_000_000;
  private static final Object VALUE = new Object();

  private List<CharSequence> filePaths;

  @Setup
  public void setup() {
    List<CharSequence> randomFilePaths = Lists.newArrayList();
    for (int i = 0; i < NUM_FILE_PATHS; i++) {
      randomFilePaths.add(generateFilePath());
    }
    this.filePaths = randomFilePaths;
  }

  @Benchmark
  @Threads(1)
  public void defaultCharSequenceMap(Blackhole blackhole) {
    CharSequenceMap<Object> map = CharSequenceMap.create();
    for (int i = 0; i < 10; i++) {
      for (CharSequence filePath : filePaths) {
        map.putIfAbsent(filePath, VALUE);
      }
    }
    blackhole.consume(map);
  }

  @Benchmark
  @Threads(1)
  public void filePathCharSequenceMap(Blackhole blackhole) {
    CharSequenceMap<Object> map = CharSequenceMap.createForFilePaths();
    for (int i = 0; i < 10; i++) {
      for (CharSequence filePath : filePaths) {
        map.putIfAbsent(filePath, VALUE);
      }
    }
    blackhole.consume(map);
  }

  private String generateFilePath() {
    int col1Value = RANDOM.nextInt(1000);
    int col2Value = RANDOM.nextInt(5000);
    String fileName = generateFileName();
    return LOCATIONS.newDataLocation(SPEC, TestHelpers.Row.of(col1Value, col2Value), fileName);
  }

  private String generateFileName() {
    int partitionId = RANDOM.nextInt(100000);
    int taskId = RANDOM.nextInt(100);
    UUID operationId = UUID.randomUUID();
    int fileCount = RANDOM.nextInt(1000);
    return String.format("%d-%d-%s-%d", partitionId, taskId, operationId, fileCount);
  }
}
