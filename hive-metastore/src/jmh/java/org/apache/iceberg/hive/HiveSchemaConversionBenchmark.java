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
package org.apache.iceberg.hive;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of converting an Iceberg {@link Schema} to the Hive
 * column list via {@link HiveSchemaUtil#convert(Schema)}. This conversion (and the recursive Hive
 * type-string building it drives) runs on every Hive table/view commit through {@code
 * HiveOperationsBase.storageDescriptor}, so its cost is paid by every engine committing to a Hive
 * catalog.
 *
 * <p>To run this benchmark: <code>
 * ./gradlew :iceberg-hive-metastore:jmh
 * -PjmhIncludeRegex=HiveSchemaConversionBenchmark
 * -PjmhOutputPath=benchmark/hive-schema-conversion-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class HiveSchemaConversionBenchmark {

  @Param({"FLAT", "NESTED_WIDE", "NESTED_DEEP"})
  private String shape;

  private Schema schema;
  private int nextId;

  @Setup
  public void setupBenchmark() {
    this.nextId = 0;
    switch (shape) {
      case "FLAT":
        this.schema = flatSchema();
        break;
      case "NESTED_WIDE":
        this.schema = nestedWideSchema();
        break;
      case "NESTED_DEEP":
        this.schema = nestedDeepSchema();
        break;
      default:
        throw new IllegalArgumentException("Unknown shape: " + shape);
    }
  }

  @Benchmark
  @Threads(1)
  public void convertSchema(Blackhole blackhole) {
    blackhole.consume(HiveSchemaUtil.convert(schema));
  }

  private int id() {
    return nextId++;
  }

  /** 30 primitive columns (incl. decimals); a control case with no nested types. */
  private Schema flatSchema() {
    List<NestedField> fields = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
      fields.add(optional(id(), "c_long_" + i, Types.LongType.get()));
      fields.add(optional(id(), "c_int_" + i, Types.IntegerType.get()));
      fields.add(optional(id(), "c_string_" + i, Types.StringType.get()));
      fields.add(optional(id(), "c_double_" + i, Types.DoubleType.get()));
      fields.add(optional(id(), "c_decimal_" + i, Types.DecimalType.of(20, 4)));
    }
    return new Schema(fields);
  }

  /** Several struct / list&lt;struct&gt; / map&lt;string,struct&gt; columns; a realistic shape. */
  private Schema nestedWideSchema() {
    List<NestedField> cols = Lists.newArrayList();
    cols.add(optional(id(), "id", Types.LongType.get()));
    cols.add(optional(id(), "name", Types.StringType.get()));
    for (int i = 0; i < 6; i++) {
      cols.add(optional(id(), "struct_" + i, recordStruct()));
      cols.add(optional(id(), "list_" + i, Types.ListType.ofOptional(id(), recordStruct())));
      cols.add(
          optional(
              id(),
              "map_" + i,
              Types.MapType.ofOptional(id(), id(), Types.StringType.get(), recordStruct())));
    }
    return new Schema(cols);
  }

  /** A single column nested 10 struct levels deep; exposes the depth-compounding cost. */
  private Schema nestedDeepSchema() {
    Type type = Types.IntegerType.get();
    for (int i = 0; i < 10; i++) {
      type =
          Types.StructType.of(
              optional(id(), "level_" + i, type),
              optional(id(), "leaf_" + i, Types.StringType.get()));
    }
    return new Schema(optional(id(), "deep", type));
  }

  private Types.StructType recordStruct() {
    return Types.StructType.of(
        optional(id(), "f_a", Types.IntegerType.get()),
        optional(id(), "f_b", Types.StringType.get()),
        optional(id(), "f_c", Types.DoubleType.get()),
        optional(id(), "f_d", Types.LongType.get()));
  }
}
