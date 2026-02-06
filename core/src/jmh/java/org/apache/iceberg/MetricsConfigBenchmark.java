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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the performance of limit fields Ids with given schema.
 *
 * <p>To run this benchmark: <code>
 * ./gradlew :iceberg-core:jmh
 * -PjmhIncludeRegex=MetricsConfigBenchmark
 * -PjmhOutputPath=benchmark/metrics-config-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class MetricsConfigBenchmark {

  private Schema schema;

  @Param({"50", "10000", "100000", "1000000"})
  private int numFields;

  @Param({"100", "10000"})
  private int limitFields;

  @Setup
  public void setupBenchmark() {
    schema = initSchema();
  }

  @Benchmark
  @Threads(1)
  public void limitFields() {
    MetricsConfig.limitFieldIds(schema, limitFields);
  }

  private Schema initSchema() {
    List<NestedField> fields = Lists.newArrayList(required(0, "id", Types.LongType.get()));
    Function<Integer, NestedField> fieldIdToNestedField =
        i ->
            optional(
                i,
                "struct" + i,
                Types.StructType.of(required(i + 1, "int" + (i + 1), Types.IntegerType.get())));
    List<NestedField> additionalCols =
        IntStream.range(1, numFields)
            .filter(i -> i % 2 != 0)
            .mapToObj(fieldIdToNestedField::apply)
            .collect(Collectors.toList());
    fields.addAll(additionalCols);
    return new Schema(fields);
  }
}
