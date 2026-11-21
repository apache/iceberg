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
package org.apache.iceberg.tornadovm.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.bench.MorTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.iceberg.gpu.GpuAggregateExec$;
import org.apache.spark.sql.iceberg.gpu.GpuAggregateStrategy;
import org.apache.spark.sql.iceberg.gpu.ProbeAggregates;
import org.apache.spark.sql.iceberg.gpu.StarAggregates;
import org.apache.spark.sql.iceberg.gpu.StarAggregates$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spark SQL aggregates over a merge-on-read table with the GPU aggregate planning on and off: the
 * same groups and, to rounding, the same sums -- whether the device ran the aggregate, declined it
 * at planning, or failed it at run time and left it to the CPU.
 */
public class TestSparkGpuAggregates {
  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  public static void startSpark() {
    assumeThat(DeviceMergeOnReadScan.isAvailable()).as("TornadoVM with the cuDF shim").isTrue();
    spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.sql.extensions", "org.apache.spark.sql.iceberg.gpu.GpuAggregateExtensions")
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config("spark.sql.iceberg.partition-reader-factory-provider", "tornadovm")
            .config("spark.sql.iceberg.rewrite-runner-provider", "tornadovm")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                System.getProperty("java.io.tmpdir") + "/iceberg-gpu-spark-test")
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static void gpu(boolean on) {
    SparkEnv.get().conf().set(GpuPartitionReaderFactoryProvider.ENABLED, Boolean.toString(on));
    // These tables are small: read them on the device anyway.
    SparkEnv.get().conf().set(GpuPartitionReaderFactoryProvider.MIN_ROWS, "0");
    // Aggregates of nullable columns are opt-in; these tests cover them.
    SparkEnv.get().conf().set(GpuAggregateStrategy.NullableValues(), Boolean.toString(on));
    SparkEnv.get().conf().set(GpuAggregateStrategy.StarEnabled(), Boolean.toString(on));
    SparkEnv.get().conf().set(ProbeAggregates.Enabled(), Boolean.toString(on));
  }

  /** A small Iceberg table of the given rows, as a temporary view. */
  private void view(
      String name,
      Schema schema,
      int files,
      long rows,
      java.util.function.LongFunction<GenericRecord> row) {
    String location = temp.resolve(name).toString();
    Table table = MorTables.create(location, schema, 2);
    List<DataFile> written = Lists.newArrayList();
    long perFile = (rows + files - 1) / files;
    for (int f = 0; f < files; f++) {
      long from = f * perFile;
      long count = Math.min(perFile, rows - from);
      written.add(MorTables.writeDataFile(table, count, i -> row.apply(from + i)));
    }
    MorTables.append(table, written);
    if (name.equals("store_sales")) {
      MorTables.deletePositions(
          table, written, ImmutableList.of(new long[] {0, 9, 1_234}, new long[] {7}, new long[0]));
      MorTables.deleteEqual(table, "id", ImmutableList.of(5L, 12_345L, 29_999L));
    }
    spark.read().format("iceberg").load(location).createOrReplaceTempView(name);
  }

  /** The query's rows, sorted, and whether the device aggregate produced them. */
  private record Answer(List<Row> rows, boolean onDevice) {}

  private static Answer run(String sql) {
    Dataset<Row> frame = spark.sql(sql);
    List<Row> rows = Lists.newArrayList(frame.collectAsList());
    rows.sort((a, b) -> a.toString().compareTo(b.toString()));
    String plan = frame.queryExecution().executedPlan().toString();
    return new Answer(rows, plan.contains("GpuIcebergAggregate"));
  }

  private static void assertSameAnswer(List<Row> gpu, List<Row> cpu) {
    assertThat(gpu).hasSameSizeAs(cpu);
    for (int index = 0; index < cpu.size(); index++) {
      Row expected = cpu.get(index);
      Row actual = gpu.get(index);
      assertThat(actual.size()).isEqualTo(expected.size());
      for (int c = 0; c < expected.size(); c++) {
        Object want = expected.get(c);
        Object got = actual.get(c);
        if (want instanceof Double wanted && got instanceof Double value) {
          assertThat(value)
              .as("row %s column %d", expected, c)
              .isCloseTo(wanted, org.assertj.core.api.Assertions.within(1e-9 * Math.abs(wanted)));
        } else {
          assertThat(got).as("row %s column %d", expected, c).isEqualTo(want);
        }
      }
    }
  }

  @Test
  public void testSameAggregatesOnTheDevice() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "flag", Types.StringType.get()),
            Types.NestedField.optional(3, "status", Types.StringType.get()),
            Types.NestedField.optional(4, "shipdate", Types.DateType.get()),
            Types.NestedField.optional(5, "price", Types.DoubleType.get()),
            Types.NestedField.optional(6, "disc", Types.DoubleType.get()),
            Types.NestedField.optional(7, "tax", Types.DoubleType.get()),
            Types.NestedField.optional(8, "qty", Types.IntegerType.get()),
            Types.NestedField.optional(9, "sparse", Types.DoubleType.get()),
            Types.NestedField.optional(10, "comment", Types.StringType.get()));
    String location = temp.resolve("lineitem").toString();
    Table table = MorTables.create(location, schema, 2);
    List<DataFile> files = Lists.newArrayList();
    String[] flags = {"A", "N", "R"};
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              20_000,
              i -> {
                long id = index * 1_000_000L + i;
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", id);
                record.setField("flag", flags[(int) (id % 3)]);
                record.setField("status", id % 7 < 3 ? "F" : "O");
                record.setField("shipdate", LocalDate.of(1992, 1, 1).plusDays(id % 2500));
                record.setField("price", 900.0 + (id % 1000) * 1.37);
                record.setField("disc", (id % 11) / 100.0);
                record.setField("tax", (id % 9) / 100.0);
                record.setField("qty", (int) (1 + id % 50));
                record.setField("sparse", id % 5 == 0 ? null : id * 0.25);
                record.setField("comment", "a comment longer than eight bytes " + (id % 4));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(new long[] {0, 5, 17, 19_999}, new long[] {3}, new long[0]));
    MorTables.deleteEqual(table, "id", ImmutableList.of(7L, 1_000_008L, 2_000_009L));
    spark.read().format("iceberg").load(location).createOrReplaceTempView("lineitem");

    // TPC-H Q1 and Q6 shapes, an empty global aggregate, int and bigint sums exact in
    // doubles, Iceberg's bucket transform as the group.
    List<String> onDevice =
        ImmutableList.of(
            "SELECT flag, status, sum(qty), sum(price), sum(price * (1 - disc)),"
                + " sum(price * (1 - disc) * (1 + tax)), avg(qty), avg(price), avg(disc), count(*)"
                + " FROM lineitem WHERE shipdate <= DATE '1998-09-02' GROUP BY flag, status",
            "SELECT count(*), sum(price * disc) FROM lineitem WHERE shipdate >= DATE '1994-01-01'"
                + " AND shipdate < DATE '1995-01-01' AND disc >= 0.05 AND disc <= 0.07"
                + " AND qty < 24",
            "SELECT count(*), sum(price), avg(tax) FROM lineitem WHERE price < 0",
            "SELECT shipdate, avg(price * qty) FROM lineitem WHERE id < 3000 GROUP BY shipdate",
            "SELECT flag, sum(id) FROM lineitem GROUP BY flag",
            "SELECT default_iceberg.system.bucket(16, id) b, count(*), sum(price), avg(disc)"
                + " FROM lineitem WHERE disc > 0.02 GROUP BY b",
            // Nullable values: SUM and AVG skip nulls, COUNT(column) counts the rest. sparse is
            // null exactly where qty % 5 == 1, so those groups sum to NULL with a count of 0.
            "SELECT flag, sum(sparse), avg(sparse), count(sparse), count(*) FROM lineitem"
                + " GROUP BY flag",
            "SELECT qty, sum(sparse), avg(sparse), count(sparse), count(*) FROM lineitem"
                + " GROUP BY qty",
            "SELECT status, sum(sparse * disc), avg(price * (1 - disc)), sum(price)"
                + " FROM lineitem WHERE shipdate > DATE '1993-01-01' GROUP BY status");
    // Declined when planned (a computed group key), or failed on the device (a group
    // string over 8 bytes): the CPU answers.
    List<String> onCpu =
        ImmutableList.of(
            // TPC-DS q54's shape: the group key a projected expression, declined without failing.
            "SELECT segment, count(*), sum(price) FROM (SELECT CAST(price / 50 AS INT) segment,"
                + " price FROM lineitem) GROUP BY segment",
            "SELECT comment, sum(price) FROM lineitem GROUP BY comment");

    List<Answer> cpu = Lists.newArrayList();
    for (String sql : onDevice) {
      cpu.add(run(sql));
    }
    for (String sql : onCpu) {
      cpu.add(run(sql));
    }
    gpu(true);
    List<Answer> device = Lists.newArrayList();
    try {
      for (String sql : onDevice) {
        device.add(run(sql));
      }
      for (String sql : onCpu) {
        device.add(run(sql));
      }
    } finally {
      gpu(false);
    }

    for (int q = 0; q < cpu.size(); q++) {
      assertThat(cpu.get(q).onDevice()).isFalse();
      assertThat(cpu.get(q).rows()).isNotEmpty();
      assertSameAnswer(device.get(q).rows(), cpu.get(q).rows());
    }
    for (int q = 0; q < onDevice.size(); q++) {
      assertThat(device.get(q).onDevice()).as(onDevice.get(q)).isTrue();
    }
    for (int q = 0; q < onCpu.size() - 1; q++) {
      assertThat(device.get(onDevice.size() + q).onDevice()).as(onCpu.get(q)).isFalse();
    }

    // By default a SUM over a column that may hold nulls stays on Spark.
    String nullable = "SELECT flag, sum(sparse) FROM lineitem GROUP BY flag";
    SparkEnv.get().conf().set(GpuPartitionReaderFactoryProvider.ENABLED, "true");
    try {
      assertThat(run(nullable).onDevice()).as("nullable values off by default").isFalse();
    } finally {
      gpu(false);
    }
  }

  /**
   * Star aggregates: a fact table under deletes, inner-joined with date, item and store dimensions
   * -- TPC-DS q3's and q22's shapes (ROLLUP, AVG of a nullable column), three joins with filters on
   * the fact and dimension tables, fact columns as group keys, a global aggregate -- the same
   * answers with the joins on the device as on Spark. A left join, an aggregate of a dimension
   * column and a fact column as a group key are declined; a dimension with a repeated key fails on
   * the device and runs on the CPU.
   */
  /**
   * A device aggregate's rows leave the driver when the query that computed them ends: they were
   * kept for the last 64 aggregates, and at TPC-DS SF100 that held 63M rows (15 GB) on the heap
   * across queries, doubling q78's time.
   */
  @Test
  public void testDeviceAggregateResultsLeaveWithTheirQuery() throws InterruptedException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "k", Types.IntegerType.get()),
            Types.NestedField.required(3, "v", Types.DoubleType.get()));
    view(
        "kept",
        schema,
        2,
        20_000,
        i -> {
          GenericRecord record = GenericRecord.create(schema);
          record.setField("id", i);
          record.setField("k", (int) (i % 1000));
          record.setField("v", i * 0.5);
          return record;
        });
    String sql = "SELECT k, sum(v), count(*) FROM kept GROUP BY k";
    Answer cpu = run(sql);
    gpu(true);
    try {
      for (int repeat = 0; repeat < 3; repeat++) {
        Answer device = run(sql);
        assertThat(device.onDevice()).isTrue();
        assertSameAnswer(device.rows(), cpu.rows());
        // The execution's end reaches the listener on Spark's bus, a moment later.
        long deadline = System.currentTimeMillis() + 10_000;
        while (GpuAggregateExec$.MODULE$.kept() > 0 && System.currentTimeMillis() < deadline) {
          Thread.sleep(20);
        }
        assertThat(GpuAggregateExec$.MODULE$.kept())
            .as("results kept after query %d", repeat)
            .isZero();
      }
    } finally {
      gpu(false);
    }
  }

  @Test
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  public void testStarAggregates() {
    Schema sales =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "date_sk", Types.LongType.get()),
            Types.NestedField.optional(3, "item_sk", Types.IntegerType.get()),
            Types.NestedField.optional(4, "store_sk", Types.LongType.get()),
            Types.NestedField.optional(5, "qty", Types.IntegerType.get()),
            Types.NestedField.optional(6, "price", Types.DecimalType.of(7, 2)),
            Types.NestedField.optional(7, "amount", Types.DoubleType.get()));
    view(
        "store_sales",
        sales,
        3,
        30_000,
        id -> {
          GenericRecord record = GenericRecord.create(sales);
          record.setField("id", id);
          record.setField("date_sk", id % 97 == 0 ? null : 2_415_022L + (id * 31) % 420);
          record.setField("item_sk", (int) (1 + (id * 17) % 230)); // items 201..230 are not in item
          record.setField("store_sk", 1 + id % 12);
          record.setField("qty", id % 13 == 0 ? null : (int) (id % 40));
          record.setField("price", java.math.BigDecimal.valueOf((id * 7919) % 99_999, 2));
          record.setField("amount", (id % 1000) * 0.5);
          return record;
        });
    Schema dates =
        new Schema(
            Types.NestedField.required(1, "d_date_sk", Types.LongType.get()),
            Types.NestedField.optional(2, "d_year", Types.LongType.get()),
            Types.NestedField.optional(3, "d_moy", Types.IntegerType.get()));
    view(
        "date_dim",
        dates,
        1,
        400,
        k -> {
          GenericRecord record = GenericRecord.create(dates);
          record.setField("d_date_sk", 2_415_022L + k);
          record.setField("d_year", 2000L + k / 120);
          record.setField("d_moy", (int) (1 + (k / 10) % 12));
          return record;
        });
    Schema items =
        new Schema(
            Types.NestedField.required(1, "i_item_sk", Types.IntegerType.get()),
            Types.NestedField.optional(2, "i_brand", Types.StringType.get()),
            Types.NestedField.optional(3, "i_class", Types.StringType.get()),
            Types.NestedField.optional(4, "i_category", Types.StringType.get()),
            Types.NestedField.optional(5, "i_product_name", Types.StringType.get()));
    view(
        "item",
        items,
        1,
        200,
        k -> {
          GenericRecord record = GenericRecord.create(items);
          record.setField("i_item_sk", (int) (1 + k));
          record.setField("i_brand", "brand #" + (k % 9));
          record.setField("i_class", "class " + (k % 4));
          record.setField("i_category", k % 11 == 0 ? null : "category " + (k % 3));
          record.setField("i_product_name", "a product name longer than eight bytes " + (k % 50));
          return record;
        });
    Schema stores =
        new Schema(
            Types.NestedField.required(1, "s_store_sk", Types.LongType.get()),
            Types.NestedField.optional(2, "s_state", Types.StringType.get()));
    view(
        "store",
        stores,
        1,
        10, // stores 11 and 12 are not in store
        k -> {
          GenericRecord record = GenericRecord.create(stores);
          record.setField("s_store_sk", 1 + k);
          record.setField("s_state", k % 3 == 0 ? "TN" : "GA");
          return record;
        });
    Schema returns =
        new Schema(
            Types.NestedField.required(1, "r_id", Types.LongType.get()),
            Types.NestedField.optional(2, "r_amt", Types.DecimalType.of(7, 2)),
            Types.NestedField.optional(3, "r_loss", Types.DecimalType.of(7, 2)));
    view(
        "rets",
        returns,
        1,
        2_000,
        k -> {
          GenericRecord record = GenericRecord.create(returns);
          record.setField("r_id", k * 13); // every 13th sale returned, ids up to 26K
          record.setField(
              "r_amt", k % 9 == 0 ? null : java.math.BigDecimal.valueOf(k * 7 % 5000, 2));
          record.setField("r_loss", java.math.BigDecimal.valueOf(k % 300, 2));
          return record;
        });
    view(
        "item_twice",
        items,
        1,
        40,
        k -> {
          GenericRecord record = GenericRecord.create(items);
          record.setField("i_item_sk", (int) (1 + k % 20));
          record.setField("i_brand", "brand #" + (k % 2));
          return record;
        });

    List<String> onDevice =
        ImmutableList.of(
            // q3
            "SELECT d_year, i_brand, sum(price) s FROM date_dim, store_sales, item"
                + " WHERE d_date_sk = date_sk AND item_sk = i_item_sk AND d_moy = 11"
                + " GROUP BY d_year, i_brand",
            // q22
            "SELECT i_product_name, i_brand, i_class, i_category, avg(qty) qoh"
                + " FROM store_sales, date_dim, item, store WHERE date_sk = d_date_sk"
                + " AND item_sk = i_item_sk AND store_sk = s_store_sk AND d_year BETWEEN 2000 AND 2001"
                + " GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)",
            // Three joins, fact and dimension filters, a dimension's key as a group key.
            "SELECT s_state, i_category, s_store_sk, count(*), sum(amount), avg(price), count(qty)"
                + " FROM store_sales JOIN item ON item_sk = i_item_sk JOIN store ON s_store_sk = store_sk"
                + " JOIN date_dim ON d_date_sk = date_sk WHERE amount > 10 AND i_brand LIKE 'brand #1%'"
                + " AND d_moy IN (1, 2, 12) GROUP BY s_state, i_category, s_store_sk",
            "SELECT sum(amount), count(*) FROM store_sales, store WHERE store_sk = s_store_sk"
                + " AND s_state = 'TN'",
            "SELECT d_year, d_moy, sum(qty) FROM store_sales, date_dim WHERE date_sk = d_date_sk"
                + " GROUP BY CUBE(d_year, d_moy)",
            // q43's and q21's shape: aggregates of a CASE on dimension columns.
            "SELECT s_state, sum(CASE WHEN d_moy = 1 THEN price END) jan,"
                + " sum(CASE WHEN d_moy = 2 THEN price ELSE 0 END) feb,"
                + " count(CASE WHEN d_moy > 6 THEN 1 END) late,"
                + " avg(CASE WHEN d_year = 2000 THEN amount END) y2000,"
                + " sum(CASE WHEN d_year = 2001 THEN qty ELSE 0 END) q2001, count(*)"
                + " FROM store_sales, date_dim, store WHERE date_sk = d_date_sk"
                + " AND store_sk = s_store_sk GROUP BY s_state",
            // The same filtered dimension read for other columns, one query after the other (q42
            // and q52): built dimensions are kept, and must not be mistaken for each other.
            "SELECT i_class, sum(amount) FROM store_sales, item WHERE item_sk = i_item_sk"
                + " AND i_category = 'category 1' GROUP BY i_class",
            "SELECT i_brand, sum(amount) FROM store_sales, item WHERE item_sk = i_item_sk"
                + " AND i_category = 'category 1' GROUP BY i_brand",
            // q11's: sums of differences and sums of columns (a null in either skips the row).
            "SELECT s_state, sum(qty - store_sk), sum(store_sk + qty), avg(qty - store_sk)"
                + " FROM store_sales, store WHERE store_sk = s_store_sk GROUP BY s_state",
            // q4's: sums and averages of a decimal expression halved, each row's half exact.
            "SELECT s_state, sum((price + price) / 2), avg((price - price) / 2), sum(price / 2)"
                + " FROM store_sales, store WHERE store_sk = s_store_sk GROUP BY s_state",
            // q17's: variances and standard deviations of an int column (nulls skipped).
            "SELECT s_state, stddev_samp(qty), var_pop(qty), stddev_pop(qty), count(qty)"
                + " FROM store_sales, store WHERE store_sk = s_store_sk GROUP BY s_state",
            // q80's: a left outer join to returns, sums of returned amounts (0 when none) and of
            // the sales' profit less the returns' loss.
            "SELECT s_state, sum(price), sum(coalesce(r_amt, 0)), sum(price - coalesce(r_loss, 0)),"
                + " count(*) FROM store_sales LEFT OUTER JOIN rets ON id = r_id"
                + " JOIN store ON store_sk = s_store_sk GROUP BY s_state",
            // q23's and q14's: left semi joins, fact keys IN a subquery's (repeated) keys.
            "SELECT s_state, sum(amount), count(*) FROM store_sales, store WHERE store_sk = s_store_sk"
                + " AND item_sk IN (SELECT item_sk FROM store_sales GROUP BY item_sk"
                + " HAVING count(*) > 130) GROUP BY s_state",
            "SELECT sum(amount), count(*) FROM store_sales WHERE item_sk IN"
                + " (SELECT i_item_sk FROM item WHERE i_brand LIKE 'brand #1%')"
                + " AND date_sk IN (SELECT d_date_sk FROM date_dim WHERE d_moy = 3)",
            // A left semi join on a dimension's key, applied with the dimension's plan.
            "SELECT i_brand, sum(amount) FROM store_sales, item WHERE item_sk = i_item_sk"
                + " AND i_item_sk IN (SELECT item_sk FROM store_sales GROUP BY item_sk"
                + " HAVING count(*) > 130) GROUP BY i_brand",
            // q23a's: sums over a UNION ALL of two stars, each branch on the device.
            "SELECT sum(v), count(*), sum(q) FROM (SELECT amount v, qty q FROM store_sales, store"
                + " WHERE store_sk = s_store_sk AND s_state = 'TN' UNION ALL SELECT amount, qty"
                + " FROM store_sales, date_dim WHERE date_sk = d_date_sk AND d_moy = 3) u",
            // q33's, q56's: grouped by a key of both branches.
            "SELECT k, sum(v), count(*) FROM (SELECT s_state k, amount v FROM store_sales, store"
                + " WHERE store_sk = s_store_sk UNION ALL SELECT i_brand, amount"
                + " FROM store_sales, item WHERE item_sk = i_item_sk) u GROUP BY k",
            // q67's value: a decimal product, nulls counted as 0.
            "SELECT i_category, i_class, sum(coalesce(price * qty, 0)) sumsales, sum(price * qty)"
                + " FROM store_sales, item WHERE item_sk = i_item_sk"
                + " GROUP BY ROLLUP(i_category, i_class)");
    List<String> onCpu =
        ImmutableList.of(
            "SELECT i_brand, count(*) FROM store_sales LEFT JOIN item ON item_sk = i_item_sk"
                + " GROUP BY i_brand",
            "SELECT i_brand, sum(d_moy) FROM store_sales, date_dim, item"
                + " WHERE date_sk = d_date_sk AND item_sk = i_item_sk GROUP BY i_brand",
            // A fact column as a group key: groups unbounded.
            "SELECT store_sk, i_brand, sum(amount) FROM store_sales, item WHERE item_sk = i_item_sk"
                + " GROUP BY store_sk, i_brand",
            // Planned on the device, failed there (item_twice repeats keys): Spark answers.
            "SELECT i_brand, sum(amount) FROM store_sales, item_twice WHERE item_sk = i_item_sk"
                + " GROUP BY i_brand");

    List<Answer> cpu = Lists.newArrayList();
    for (String sql : onDevice) {
      cpu.add(run(sql));
    }
    for (String sql : onCpu) {
      cpu.add(run(sql));
    }
    gpu(true);
    List<Answer> device = Lists.newArrayList();
    try {
      for (String sql : onDevice) {
        device.add(run(sql));
      }
      for (String sql : onCpu) {
        device.add(run(sql));
      }
    } finally {
      gpu(false);
    }
    for (int q = 0; q < cpu.size(); q++) {
      assertThat(cpu.get(q).onDevice()).isFalse();
      assertThat(cpu.get(q).rows()).isNotEmpty();
      assertSameAnswer(device.get(q).rows(), cpu.get(q).rows());
    }
    for (int q = 0; q < onDevice.size(); q++) {
      assertThat(device.get(q).onDevice()).as(onDevice.get(q)).isTrue();
    }
    for (int q = 0; q < onCpu.size() - 1; q++) {
      assertThat(device.get(onDevice.size() + q).onDevice()).as(onCpu.get(q)).isFalse();
    }
  }

  /**
   * Probe aggregates, TPC-DS q72's shape: stock rows (the largest table) joined with sales on the
   * item and on the week -- a date dimension's attribute on each side -- where the stock on hand is
   * below the quantity sold; the sales side with its own dimension filter, a condition across its
   * two dates and a left outer join to promotions. Grouped by the item's description, the
   * warehouse's name and the week, counting rows; and sums of both sides' columns over the join
   * (the sales side's summed per stock row): the same answers on the device as on Spark.
   */
  /**
   * Adaptive execution plans an aggregate again after its joins become query stages. A star
   * aggregate that failed on the device must be recognised in both shapes: keyed by the plan with
   * the stage as a leaf, TPC-DS q67 was accepted again in every run and fell back to the CPU each
   * time (17 s against Spark's 5.6 s).
   */
  @Test
  public void testStarFailureMemorySurvivesQueryStages() {
    Schema facts =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v", Types.IntegerType.get()));
    view(
        "stage_facts",
        facts,
        2,
        1_000,
        id -> {
          GenericRecord record = GenericRecord.create(facts);
          record.setField("id", id);
          record.setField("k", (int) (id % 10));
          record.setField("v", (int) id);
          return record;
        });
    Schema dims =
        new Schema(
            Types.NestedField.required(1, "k", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    view(
        "stage_dims",
        dims,
        1,
        10,
        id -> {
          GenericRecord record = GenericRecord.create(dims);
          record.setField("k", (int) id);
          record.setField("name", "n" + id);
          return record;
        });
    org.apache.spark.sql.catalyst.plans.logical.LogicalPlan agg =
        spark
            .sql(
                "SELECT d.name, SUM(f.v) FROM stage_facts f JOIN stage_dims d ON f.k = d.k GROUP BY d.name")
            .queryExecution()
            .optimizedPlan();
    org.apache.spark.sql.catalyst.plans.logical.Join join =
        (org.apache.spark.sql.catalyst.plans.logical.Join)
            agg.collectFirst(
                    new scala.runtime.AbstractPartialFunction<
                        org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
                        org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>() {
                      @Override
                      public boolean isDefinedAt(
                          org.apache.spark.sql.catalyst.plans.logical.LogicalPlan p) {
                        return p instanceof org.apache.spark.sql.catalyst.plans.logical.Join;
                      }

                      @Override
                      public org.apache.spark.sql.catalyst.plans.logical.LogicalPlan apply(
                          org.apache.spark.sql.catalyst.plans.logical.LogicalPlan p) {
                        return p;
                      }
                    })
                .get();
    org.apache.spark.sql.catalyst.plans.logical.LogicalPlan dimension = join.right();
    org.apache.spark.sql.execution.adaptive.LogicalQueryStage stage =
        new org.apache.spark.sql.execution.adaptive.LogicalQueryStage(
            dimension,
            spark
                .sessionState()
                .executePlan(dimension, org.apache.spark.sql.execution.CommandExecutionMode.ALL())
                .executedPlan());
    org.apache.spark.sql.catalyst.plans.logical.LogicalPlan staged =
        agg.transformUp(
            new scala.runtime.AbstractPartialFunction<
                org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
                org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>() {
              @Override
              public boolean isDefinedAt(
                  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan p) {
                return p == dimension;
              }

              @Override
              public org.apache.spark.sql.catalyst.plans.logical.LogicalPlan apply(
                  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan p) {
                return stage;
              }
            });
    assertThat(staged.toString()).contains("LogicalQueryStage");
    assertThat(StarAggregates$.MODULE$.identity(staged))
        .isEqualTo(StarAggregates$.MODULE$.identity(agg));
  }

  @Test
  public void testProbeAggregates() {
    Schema stock =
        new Schema(
            Types.NestedField.required(1, "inv_date_sk", Types.LongType.get()),
            Types.NestedField.required(2, "inv_item_sk", Types.LongType.get()),
            Types.NestedField.required(3, "inv_wh_sk", Types.LongType.get()),
            Types.NestedField.optional(4, "inv_qoh", Types.IntegerType.get()));
    view(
        "stock",
        stock,
        2,
        40_000,
        id -> {
          GenericRecord record = GenericRecord.create(stock);
          record.setField("inv_date_sk", 1000L + 7 * ((id / 400) % 30)); // one row a week
          record.setField("inv_item_sk", 1 + (id % 100));
          record.setField("inv_wh_sk", 1 + (id / 100) % 4);
          record.setField("inv_qoh", id % 17 == 0 ? null : (int) ((id * 7) % 50));
          return record;
        });
    Schema sales =
        new Schema(
            Types.NestedField.required(1, "cs_sold_date_sk", Types.LongType.get()),
            Types.NestedField.required(2, "cs_ship_date_sk", Types.LongType.get()),
            Types.NestedField.required(3, "cs_item_sk", Types.LongType.get()),
            Types.NestedField.required(4, "cs_qty", Types.LongType.get()),
            Types.NestedField.optional(5, "cs_promo_sk", Types.LongType.get()));
    view(
        "sales",
        sales,
        1,
        3_000,
        id -> {
          GenericRecord record = GenericRecord.create(sales);
          long sold = 1000 + (id * 13) % 210;
          record.setField("cs_sold_date_sk", sold);
          record.setField("cs_ship_date_sk", sold + (id % 9));
          record.setField("cs_item_sk", 1 + (id * 31) % 120); // items 101..120 have no stock
          record.setField("cs_qty", 1 + (id % 60));
          record.setField("cs_promo_sk", id % 5 == 0 ? null : 1 + id % 7);
          return record;
        });
    Schema dates =
        new Schema(
            Types.NestedField.required(1, "dd_sk", Types.LongType.get()),
            Types.NestedField.optional(2, "dd_week", Types.LongType.get()),
            Types.NestedField.optional(3, "dd_year", Types.LongType.get()),
            Types.NestedField.optional(4, "dd_day", Types.IntegerType.get()));
    view(
        "dd",
        dates,
        1,
        400,
        k -> {
          GenericRecord record = GenericRecord.create(dates);
          record.setField("dd_sk", 1000 + k);
          record.setField("dd_week", 5000 + k / 7);
          record.setField("dd_year", 2000 + k / 100);
          record.setField("dd_day", (int) k);
          return record;
        });
    Schema items =
        new Schema(
            Types.NestedField.required(1, "it_sk", Types.LongType.get()),
            Types.NestedField.optional(2, "it_desc", Types.StringType.get()));
    view(
        "it",
        items,
        1,
        100,
        k -> {
          GenericRecord record = GenericRecord.create(items);
          record.setField("it_sk", 1 + k);
          record.setField("it_desc", "item description " + (k % 37));
          return record;
        });
    Schema warehouses =
        new Schema(
            Types.NestedField.required(1, "wh_sk", Types.LongType.get()),
            Types.NestedField.optional(2, "wh_name", Types.StringType.get()));
    view(
        "wh",
        warehouses,
        1,
        4,
        k -> {
          GenericRecord record = GenericRecord.create(warehouses);
          record.setField("wh_sk", 1 + k);
          record.setField("wh_name", "warehouse " + k);
          return record;
        });
    Schema promotions = new Schema(Types.NestedField.required(1, "p_sk", Types.LongType.get()));
    view(
        "promo",
        promotions,
        1,
        6,
        k -> {
          GenericRecord record = GenericRecord.create(promotions);
          record.setField("p_sk", 1 + k);
          return record;
        });

    String sql =
        "SELECT it_desc, wh_name, d1.dd_week, count(CASE WHEN p_sk IS NULL THEN 1 ELSE 0 END) np,"
            + " count(*) total FROM sales JOIN stock ON cs_item_sk = inv_item_sk"
            + " JOIN wh ON wh_sk = inv_wh_sk JOIN it ON it_sk = cs_item_sk"
            + " JOIN dd d1 ON cs_sold_date_sk = d1.dd_sk JOIN dd d2 ON inv_date_sk = d2.dd_sk"
            + " JOIN dd d3 ON cs_ship_date_sk = d3.dd_sk"
            + " LEFT OUTER JOIN promo ON cs_promo_sk = p_sk"
            + " WHERE d1.dd_week = d2.dd_week AND inv_qoh < cs_qty AND d3.dd_day > d1.dd_day + 2"
            + " AND d1.dd_year = 2000 GROUP BY it_desc, wh_name, d1.dd_week";
    // Sums of both sides' columns over the join: the build side's summed per stock row.
    String sums =
        "SELECT wh_name, sum(inv_qoh), sum(cs_qty), avg(cs_qty), stddev_samp(cs_qty),"
            + " stddev_samp(inv_qoh), count(cs_promo_sk), count(*) FROM sales"
            + " JOIN stock ON cs_item_sk = inv_item_sk JOIN wh ON wh_sk = inv_wh_sk"
            + " JOIN dd d1 ON cs_sold_date_sk = d1.dd_sk JOIN dd d2 ON inv_date_sk = d2.dd_sk"
            + " WHERE d1.dd_week = d2.dd_week AND inv_qoh < cs_qty GROUP BY wh_name";
    // q64's cs_ui: a composite-key join, grouped by a fact column, a build-side decimal sum.
    String grouped =
        "SELECT inv_item_sk, sum(inv_qoh), sum(cast(cs_qty AS decimal(9, 2))), count(*)"
            + " FROM stock JOIN sales ON cs_item_sk = inv_item_sk AND cs_sold_date_sk = inv_date_sk"
            + " GROUP BY inv_item_sk";
    for (String query : ImmutableList.of(sql, sums, grouped)) {
      Answer cpu = run(query);
      gpu(true);
      // These tables are small: a table over 1,000 rows is a fact, as sales is.
      SparkEnv.get().conf().set(StarAggregates.MaxDimensionRows(), "1000");
      Answer device;
      try {
        device = run(query);
      } finally {
        gpu(false);
        SparkEnv.get().conf().remove(StarAggregates.MaxDimensionRows());
      }
      assertThat(cpu.onDevice()).isFalse();
      assertThat(cpu.rows()).isNotEmpty();
      assertThat(device.onDevice()).as(query).isTrue();
      assertSameAnswer(device.rows(), cpu.rows());
    }
  }

  /**
   * Decimal columns, as TPC-DS has them: rows read through the GPU reader, and SUM, AVG and group
   * keys of decimals from the device aggregate, equal to Spark's exact decimal answers.
   */
  @Test
  public void testDecimalReadsAndAggregates() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "price", Types.DecimalType.of(7, 2)),
            Types.NestedField.optional(3, "net", Types.DecimalType.of(15, 2)),
            Types.NestedField.optional(4, "band", Types.DecimalType.of(3, 1)),
            Types.NestedField.optional(5, "store", Types.IntegerType.get()));
    String location = temp.resolve("sales").toString();
    Table table = MorTables.create(location, schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              10_000,
              i -> {
                long id = index * 1_000_000L + i;
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", id);
                record.setField(
                    "price", java.math.BigDecimal.valueOf((id * 7919) % 9_999_999 - 4_999_999, 2));
                record.setField(
                    "net", java.math.BigDecimal.valueOf(9_999_999_999L - id * 1_234L, 2));
                record.setField("band", java.math.BigDecimal.valueOf(id % 5, 1));
                record.setField("store", (int) (id % 7));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(new long[] {1, 2, 3}, new long[0], new long[] {9_999}));
    MorTables.deleteEqual(table, "id", ImmutableList.of(11L, 1_000_012L));
    spark.read().format("iceberg").load(location).createOrReplaceTempView("sales");

    List<String> onDevice =
        ImmutableList.of(
            "SELECT band, store, sum(price), avg(price), sum(net), avg(net), count(*) FROM sales"
                + " WHERE price > -1234.56 GROUP BY band, store",
            "SELECT sum(price), avg(net), count(*) FROM sales WHERE band = 0.3");
    String rows = "SELECT * FROM sales WHERE price < 100.00";

    List<Answer> cpu = Lists.newArrayList();
    for (String sql : onDevice) {
      cpu.add(run(sql));
    }
    Answer cpuRows = run(rows);
    gpu(true);
    List<Answer> device = Lists.newArrayList();
    Answer gpuRows;
    long tasks = GpuPartitionReader.TASKS.get();
    try {
      for (String sql : onDevice) {
        device.add(run(sql));
      }
      gpuRows = run(rows);
    } finally {
      gpu(false);
    }
    for (int q = 0; q < onDevice.size(); q++) {
      assertThat(cpu.get(q).rows()).isNotEmpty();
      assertThat(device.get(q).onDevice()).as(onDevice.get(q)).isTrue();
      assertThat(device.get(q).rows()).containsExactlyElementsOf(cpu.get(q).rows());
    }
    assertThat(GpuPartitionReader.TASKS.get()).as("GPU reader tasks").isGreaterThan(tasks);
    assertThat(cpuRows.rows()).isNotEmpty();
    assertThat(gpuRows.rows()).containsExactlyElementsOf(cpuRows.rows());
  }
}
