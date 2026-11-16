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
package org.apache.iceberg.spark.source.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceBenchmark;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads single leaves at different depths of a wide nested struct, plus struct selections. Pruning
 * shows up in the bytes read per op: a depth-3 long leaf costs about as much as the top-level long,
 * and orders of magnitude less than the enclosing struct.
 */
@Fork(1)
public class IcebergSourceDeepNestedParquetDataReadBenchmark extends IcebergSourceBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergSourceDeepNestedParquetDataReadBenchmark.class);
  private static final int NUM_FILES = 10;
  private static final long NUM_ROWS = 100_000L;
  private static final int NUM_WIDE = 8;
  // the smallest arm reads a compressed long column, which is well above this
  private static final long MIN_BYTES_PER_OP = 64 * 1024;

  private boolean countersUnavailable = false;

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  @Override
  protected Table initTable() {
    List<Types.NestedField> l2Fields = Lists.newArrayList();
    l2Fields.add(required(20, "x", Types.StringType.get()));
    for (int i = 0; i < NUM_WIDE; i++) {
      l2Fields.add(required(21 + i, "w" + i, Types.StringType.get()));
    }

    l2Fields.add(
        optional(
            40,
            "l3",
            Types.StructType.of(
                required(41, "leaf", Types.LongType.get()),
                required(42, "big", Types.StringType.get()))));

    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "person",
                Types.StructType.of(
                    required(3, "name", Types.StringType.get()),
                    required(4, "home_address", address(5)),
                    required(10, "work_address", address(11)),
                    optional(19, "l2", Types.StructType.of(l2Fields)))));

    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return tables.create(schema, PartitionSpec.unpartitioned(), properties, newTableLocation());
  }

  private static Types.StructType address(int firstId) {
    return Types.StructType.of(
        required(firstId, "street", Types.StringType.get()),
        required(firstId + 1, "city", Types.StringType.get()),
        required(firstId + 2, "state", Types.StringType.get()),
        required(firstId + 3, "zip", Types.StringType.get()));
  }

  @Setup
  public void setupBenchmark() {
    setupSpark();
    appendData();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void readId(BytesReadCounters counters) {
    read("readId", counters, "id");
  }

  @Benchmark
  @Threads(1)
  public void readDepth1Leaf(BytesReadCounters counters) {
    read("readDepth1Leaf", counters, "person.name");
  }

  @Benchmark
  @Threads(1)
  public void readDepth2Leaf(BytesReadCounters counters) {
    read("readDepth2Leaf", counters, "person.home_address.city");
  }

  @Benchmark
  @Threads(1)
  public void readDepth3Leaf(BytesReadCounters counters) {
    read("readDepth3Leaf", counters, "person.l2.l3.leaf");
  }

  @Benchmark
  @Threads(1)
  public void readStructAndChild(BytesReadCounters counters) {
    read("readStructAndChild", counters, "person.l2", "person.l2.x");
  }

  @Benchmark
  @Threads(1)
  public void readWholeStruct(BytesReadCounters counters) {
    read("readWholeStruct", counters, "person");
  }

  private void read(String arm, BytesReadCounters counters, String... columns) {
    long before = bytesRead();
    Dataset<Row> df = spark().read().format("iceberg").load(table().location()).selectExpr(columns);
    materialize(df);
    long after = bytesRead();

    if (before < 0 || after < 0) {
      if (!countersUnavailable) {
        countersUnavailable = true;
        LOG.warn(
            "bytes read per op unavailable: /proc/self/io is unreadable, timing is unaffected");
      }
      return;
    }

    counters.bytes = after - before;
    if (counters.bytes < MIN_BYTES_PER_OP) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "%s read %d bytes, the counter did not observe the read",
              arm,
              counters.bytes));
    }
  }

  // rchar counts every read syscall of this JVM, including vectored reads that skip FS statistics
  private static long bytesRead() {
    try {
      return Files.readAllLines(Paths.get("/proc/self/io")).stream()
          .filter(line -> line.startsWith("rchar:"))
          .mapToLong(line -> Long.parseLong(line.substring("rchar:".length()).trim()))
          .sum();
    } catch (IOException | RuntimeException e) {
      return -1;
    }
  }

  private void appendData() {
    StringBuilder wide = new StringBuilder();
    for (int i = 0; i < NUM_WIDE; i++) {
      wide.append(String.format(Locale.ROOT, ", 'w%d', %s", i, randomString("w" + i)));
    }

    String person =
        "named_struct('name', "
            + randomString("name")
            + ", 'home_address', "
            + addressExpr("h")
            + ", 'work_address', "
            + addressExpr("w")
            + ", 'l2', IF(id % 4 = 0, NULL, named_struct('x', "
            + randomString("x")
            + wide
            + ", 'l3', IF(id % 8 = 1, NULL, named_struct('leaf', id, 'big', "
            + randomString("big")
            + "))))) AS person";

    for (int fileNum = 0; fileNum < NUM_FILES; fileNum++) {
      Dataset<Row> df =
          spark().range(fileNum * NUM_ROWS, (fileNum + 1) * NUM_ROWS).selectExpr("id", person);
      appendAsFile(df);
    }
  }

  private static String addressExpr(String prefix) {
    return String.format(
        Locale.ROOT,
        "named_struct('street', %s, 'city', %s, 'state', %s, 'zip', %s)",
        randomString(prefix + "s"),
        randomString(prefix + "c"),
        randomString(prefix + "t"),
        randomString(prefix + "z"));
  }

  // 64 hex chars that differ per row, so compression cannot shrink the pages they sit in
  private static String randomString(String salt) {
    return String.format(Locale.ROOT, "sha2(concat('%s', CAST(id AS STRING)), 256)", salt);
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static class BytesReadCounters {
    public long bytes;

    @Setup(Level.Invocation)
    public void reset() {
      bytes = 0;
    }
  }
}
