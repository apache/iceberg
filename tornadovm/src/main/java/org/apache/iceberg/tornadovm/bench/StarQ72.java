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
package org.apache.iceberg.tornadovm.bench;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.tornadovm.CpuMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.GroupedAggregate;
import org.apache.iceberg.tornadovm.join.CpuStarAggregation;
import org.apache.iceberg.tornadovm.join.DeviceDimension;
import org.apache.iceberg.tornadovm.join.DeviceProbe;
import org.apache.iceberg.tornadovm.join.StarAggregation;

/**
 * TPC-DS q72 through the Java API, the joins on the device: catalog sales filtered by their
 * dimensions are reduced to (item, week, quantity) build rows, and every inventory row of the same
 * item and week -- the week a date dimension's attribute, which Spark joins only after the item
 * join's billion rows -- counts once a sale whose quantity exceeds its quantity on hand.
 *
 * <pre>StarQ72 &lt;warehouse&gt; &lt;runs&gt; &lt;out&gt;</pre>
 */
public final class StarQ72 {
  private StarQ72() {}

  private static final int WEEK_BITS = 64;

  // -Dstar.cpu=true runs both star aggregates on the CPU (CpuStarAggregation, star.threads threads,
  // every core by default): the same dimensions, probe and grouping as the device.
  private static final boolean CPU = Boolean.getBoolean("star.cpu");
  private static final int THREADS =
      Integer.getInteger("star.threads", Runtime.getRuntime().availableProcessors());

  private static GroupedAggregate star(
      DeviceMergeOnReadScan device,
      Table table,
      List<DeviceDimension> dimensions,
      List<DeviceDimension.Key> keys,
      List<DeviceValue> values,
      DeviceProbe probe,
      List<StarAggregation.ProbeSum> sums) {
    return CPU
        ? CpuStarAggregation.run(
            CpuMergeOnReadScan.of(table, THREADS), dimensions, keys, values, probe, sums)
        : StarAggregation.run(device, dimensions, keys, values, probe, sums);
  }

  /** A dimension's rows as maps of key to the named columns. */
  private static Map<Long, Record> rows(String warehouse, String table, String... columns)
      throws IOException {
    Map<Long, Record> rows = Maps.newHashMap();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(MorTables.load(warehouse + "/" + table)).select(columns).build()) {
      for (Record r : records) {
        Object key = r.getField(columns[0]);
        if (key != null) {
          rows.put(((Number) key).longValue(), r.copy());
        }
      }
    }
    return rows;
  }

  private static DeviceDimension keep(
      String factColumn, Map<Long, Record> rows, Predicate<Record> keep) {
    long first = rows.keySet().stream().mapToLong(Long::longValue).min().orElse(0);
    long last = rows.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
    boolean[] kept = new boolean[(int) (last - first + 1)];
    rows.forEach((k, r) -> kept[(int) (k - first)] = keep.test(r));
    return DeviceDimension.of(factColumn, (int) first, kept);
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    int runs = Integer.parseInt(args[1]);
    Table sales = MorTables.load(warehouse + "/catalog_sales");
    Table inventory = MorTables.load(warehouse + "/inventory");
    try (DeviceMergeOnReadScan salesScan =
            DeviceMergeOnReadScan.of(sales, Expressions.alwaysTrue());
        DeviceMergeOnReadScan inventoryScan =
            DeviceMergeOnReadScan.of(inventory, Expressions.alwaysTrue())) {
      for (int run = 0; run < runs; run++) {
        long t0 = System.nanoTime();
        Map<Long, Record> dates =
            rows(warehouse, "date_dim", "d_date_sk", "d_week_seq", "d_year", "d_date");
        Map<Long, Record> households =
            rows(warehouse, "household_demographics", "hd_demo_sk", "hd_buy_potential");
        Map<Long, Record> customers =
            rows(warehouse, "customer_demographics", "cd_demo_sk", "cd_marital_status");
        Map<Long, Record> items = rows(warehouse, "item", "i_item_sk", "i_item_desc");
        Map<Long, Record> warehouses =
            rows(warehouse, "warehouse", "w_warehouse_sk", "w_warehouse_name");
        long t1 = System.nanoTime();

        // Sales: d1 in 1999, the demographics, a ship date; grouped by the row's own columns.
        GroupedAggregate sold =
            star(
                salesScan,
                sales,
                ImmutableList.of(
                    keep(
                        "cs_sold_date_sk",
                        dates,
                        r -> Long.valueOf(1999).equals(number(r, "d_year"))),
                    keep(
                        "cs_bill_hdemo_sk",
                        households,
                        r -> ">10000".equals(r.getField("hd_buy_potential"))),
                    keep(
                        "cs_bill_cdemo_sk",
                        customers,
                        r -> "D".equals(r.getField("cd_marital_status"))),
                    keep("cs_ship_date_sk", dates, r -> true)),
                ImmutableList.of(
                    DeviceDimension.Key.column("cs_item_sk"),
                    DeviceDimension.Key.column("cs_sold_date_sk"),
                    DeviceDimension.Key.column("cs_ship_date_sk"),
                    DeviceDimension.Key.column("cs_quantity")),
                ImmutableList.of(DeviceValue.of("cs_quantity")),
                null,
                ImmutableList.of());
        long t2 = System.nanoTime();

        // Build rows: (item, week) and the quantity, once a sale shipped more than 5 days after.
        long firstWeek = Long.MAX_VALUE;
        List<long[]> build = Lists.newArrayList();
        for (Map.Entry<List<Object>, double[]> e : sold.groups().entrySet()) {
          List<Object> key = e.getKey();
          Record d1 = dates.get(((Number) key.get(1)).longValue());
          Record d3 = dates.get(((Number) key.get(2)).longValue());
          if (key.get(3) == null || !day(d3).isAfter(day(d1).plusDays(5))) {
            continue;
          }
          long week = number(d1, "d_week_seq");
          firstWeek = Math.min(firstWeek, week);
          for (int n = 0; n < (int) e.getValue()[1]; n++) {
            build.add(
                new long[] {
                  ((Number) key.get(0)).longValue(), week, ((Number) key.get(3)).longValue()
                });
          }
        }
        long[] keys = new long[build.size()];
        long[] values = new long[build.size()];
        long lastWeek = firstWeek;
        for (int b = 0; b < keys.length; b++) {
          long[] row = build.get(b);
          keys[b] = row[0] * WEEK_BITS + (row[1] - firstWeek);
          values[b] = row[2];
          lastWeek = Math.max(lastWeek, row[1]);
        }
        long weeks = lastWeek - firstWeek + 1;
        if (weeks > WEEK_BITS) {
          throw new IllegalStateException(weeks + " weeks");
        }
        long t3 = System.nanoTime();

        // Inventory: d2's week as an attribute code, the item's description and the warehouse's
        // name as attributes; the probe joins (item, week) where quantity on hand < sold.
        long lowWeek = firstWeek;
        long highWeek = lastWeek;
        DeviceDimension d2 =
            keep(
                "inv_date_sk",
                dates,
                r -> {
                  long week = number(r, "d_week_seq");
                  return week >= lowWeek && week <= highWeek;
                });
        int[] weekCodes =
            codes(
                dates,
                r -> {
                  long week = number(r, "d_week_seq");
                  return (int) (week >= lowWeek && week <= highWeek ? week - lowWeek : 0);
                });
        List<String> descs = Lists.newArrayList();
        int[] descCodes = dictionary(items, "i_item_desc", descs);
        List<String> names = Lists.newArrayList();
        int[] nameCodes = dictionary(warehouses, "w_warehouse_name", names);
        GroupedAggregate counts =
            star(
                inventoryScan,
                inventory,
                ImmutableList.of(
                    d2.withAttribute(weekCodes),
                    keep("inv_warehouse_sk", warehouses, r -> true).withAttribute(nameCodes),
                    keep("inv_item_sk", items, r -> true).withAttribute(descCodes)),
                ImmutableList.of(
                    DeviceDimension.Key.attribute(2, 0),
                    DeviceDimension.Key.attribute(1, 0),
                    DeviceDimension.Key.attribute(0, 0)),
                ImmutableList.of(DeviceValue.of("inv_quantity_on_hand")),
                DeviceProbe.of(keys, values)
                    .keyColumn("inv_item_sk", WEEK_BITS)
                    .keyAttribute(0, 0, 1)
                    .where("inv_quantity_on_hand", DeviceProbe.Comparison.LESS),
                ImmutableList.of());
        long t4 = System.nanoTime();

        List<Object[]> result = Lists.newArrayList();
        for (Map.Entry<List<Object>, double[]> e : counts.groups().entrySet()) {
          List<Object> key = e.getKey();
          long total = (long) e.getValue()[1];
          result.add(
              new Object[] {
                descs.get(((Number) key.get(0)).intValue()),
                names.get(((Number) key.get(1)).intValue()),
                ((Number) key.get(2)).longValue() + lowWeek,
                total
              });
        }
        Comparator<Object> nullsFirst =
            Comparator.nullsFirst((a, b) -> ((Comparable) a).compareTo(b));
        result.sort(
            (a, b) -> {
              int cmp = Long.compare((Long) b[3], (Long) a[3]);
              for (int i = 0; cmp == 0 && i < 3; i++) {
                cmp = nullsFirst.compare(a[i], b[i]);
              }
              return cmp;
            });
        long t5 = System.nanoTime();
        System.out.printf(
            Locale.ROOT,
            "run=%d dims_ms=%.1f sales_ms=%.1f build_ms=%.1f inventory_ms=%.1f host_ms=%.1f total_ms=%.1f build_rows=%d groups=%d | %s%n",
            run,
            (t1 - t0) / 1e6,
            (t2 - t1) / 1e6,
            (t3 - t2) / 1e6,
            (t4 - t3) / 1e6,
            (t5 - t4) / 1e6,
            (t5 - t0) / 1e6,
            keys.length,
            result.size(),
            inventoryScan.metrics());
        if (run == runs - 1) {
          try (PrintWriter out = new PrintWriter(args[2], StandardCharsets.UTF_8)) {
            for (Object[] row : result.subList(0, Math.min(100, result.size()))) {
              out.printf(
                  Locale.ROOT,
                  "%s|%s|%s|%s|%s|%s%n",
                  row[0],
                  row[1],
                  row[2],
                  row[3],
                  row[3],
                  row[3]);
            }
          }
        }
      }
    }
  }

  private static long number(Record record, String column) {
    Object value = record.getField(column);
    return value == null ? Long.MIN_VALUE : ((Number) value).longValue();
  }

  private static LocalDate day(Record record) {
    return (LocalDate) record.getField("d_date");
  }

  /** A code a key from 0, over the rows' key range. */
  private static int[] codes(Map<Long, Record> rows, ToIntFunction<Record> code) {
    long first = rows.keySet().stream().mapToLong(Long::longValue).min().orElse(0);
    long last = rows.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
    int[] codes = new int[(int) (last - first + 1)];
    rows.forEach((k, r) -> codes[(int) (k - first)] = code.applyAsInt(r));
    return codes;
  }

  /** Dictionary codes of a string column, its values in {@code values}. */
  private static int[] dictionary(Map<Long, Record> rows, String column, List<String> values) {
    Map<String, Integer> index = Maps.newHashMap();
    return codes(
        rows,
        r ->
            index.computeIfAbsent(
                (String) r.getField(column),
                v -> {
                  values.add(v);
                  return values.size() - 1;
                }));
  }
}
