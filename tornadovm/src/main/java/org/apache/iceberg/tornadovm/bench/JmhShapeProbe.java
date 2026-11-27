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

import java.io.File;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.GpuBenchmarks;
import org.apache.iceberg.types.Types;

/**
 * The shape of Iceberg's delete benchmarks (one 10M-row file, the flat seven-column schema), to
 * split the device scan's time: materialize and read every value, materialize only, and a device
 * count.
 */
public final class JmhShapeProbe {
  private JmhShapeProbe() {}

  public static void main(String[] args) {
    String location = new File(args[0]).getAbsolutePath();
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "longCol", Types.LongType.get()),
            Types.NestedField.required(2, "intCol", Types.IntegerType.get()),
            Types.NestedField.required(3, "floatCol", Types.FloatType.get()),
            Types.NestedField.optional(4, "doubleCol", Types.DoubleType.get()),
            Types.NestedField.optional(6, "dateCol", Types.DateType.get()),
            Types.NestedField.optional(7, "timestampCol", Types.TimestampType.withZone()),
            Types.NestedField.optional(8, "stringCol", Types.StringType.get()));
    Table table;
    if (new File(location, "metadata").exists()) {
      table = MorTables.load(location);
    } else {
      table = MorTables.create(location, schema, 2);
      LocalDate date = LocalDate.of(2026, 10, 10);
      OffsetDateTime time = date.atStartOfDay().atOffset(ZoneOffset.UTC);
      GenericRecord template = GenericRecord.create(schema);
      DataFile file =
          MorTables.writeDataFile(
              table,
              10_000_000L,
              i -> {
                GenericRecord row = template.copy();
                row.set(0, i);
                row.set(1, (int) (i % Integer.MAX_VALUE));
                row.set(2, (float) i);
                row.set(3, (double) i);
                row.set(4, date);
                row.set(5, time);
                row.set(6, date.toString());
                return row;
              });
      MorTables.append(table, List.of(file));
    }
    List<String> columns =
        ImmutableList.of(
            "longCol", "intCol", "floatCol", "doubleCol", "dateCol", "timestampCol", "stringCol");
    try (DeviceMergeOnReadScan scan = DeviceMergeOnReadScan.of(table)) {
      for (int run = 0; run < 6; run++) {
        long start = System.nanoTime();
        long rows = GpuBenchmarks.materializeAll(scan, schema, new AtomicLong());
        double all = (System.nanoTime() - start) / 1e6;
        String metrics = scan.metrics().toString();
        start = System.nanoTime();
        long same = scan.materialize(columns, batch -> {});
        double copyOnly = (System.nanoTime() - start) / 1e6;
        start = System.nanoTime();
        long counted =
            scan.aggregate(ImmutableList.of(), ImmutableList.of(DeviceValue.of("intCol"))).rows();
        double device = (System.nanoTime() - start) / 1e6;
        System.out.printf(
            "run %d: materialize+read %.0f ms | materialize only %.0f ms | device count %.0f ms | rows %d %d %d | %s%n",
            run, all, copyOnly, device, rows, same, counted, metrics);
      }
    }
  }
}
