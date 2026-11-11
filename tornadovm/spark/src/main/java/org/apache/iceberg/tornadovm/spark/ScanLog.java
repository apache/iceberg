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

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Opt-in timing of every columnar task group, on the device or on Iceberg's reader: one CSV line
 * per task group in {@code <dir>/scans-<pid>.csv} when {@value #DIR} names a directory. Measuring
 * both engines on the same scans is how the read offload decision is calibrated.
 */
final class ScanLog {
  /** The Spark configuration key naming the directory of the scan logs. */
  static final String DIR = "spark.iceberg.gpu.scanLog";

  static final String HEADER =
      "table,engine,files,rows,columns,bytes,deleteFiles,batches,ms,eqDeleteFiles,eqDeleteRecords";

  private static final Object LOCK = new Object();

  private ScanLog() {}

  /** The reader, timed if scan logging is on. */
  static PartitionReader<ColumnarBatch> timed(
      PartitionReader<ColumnarBatch> reader,
      String engine,
      Table table,
      ScanTaskGroup<FileScanTask> group,
      Schema schema) {
    SparkEnv env = SparkEnv.get();
    String dir = env == null ? null : env.conf().get(DIR, null);
    if (dir == null) {
      return reader;
    }
    long rows = 0;
    long bytes = 0;
    long deletes = 0;
    // The equality-delete files the group applies (each once, however many of its files it covers)
    // and their records: the size of the key set the CPU reader probes every row against.
    java.util.Map<String, Long> equality = new java.util.HashMap<>();
    for (FileScanTask task : group.tasks()) {
      rows += task.file().recordCount();
      bytes += task.length();
      deletes += task.deletes().size();
      for (org.apache.iceberg.DeleteFile delete : task.deletes()) {
        if (delete.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES) {
          equality.put(delete.location(), delete.recordCount());
        }
      }
    }
    String suffix =
        String.format(
            Locale.ROOT,
            "%d,%d",
            equality.size(),
            equality.values().stream().mapToLong(Long::longValue).sum());
    String prefix =
        String.format(
            Locale.ROOT,
            "%s,%s,%d,%d,%d,%d,%d",
            table.name(),
            engine,
            group.tasks().size(),
            rows,
            schema.columns().size(),
            bytes,
            deletes);
    return new Timed(reader, Paths.get(dir), prefix, suffix);
  }

  private static final class Timed implements PartitionReader<ColumnarBatch> {
    private final PartitionReader<ColumnarBatch> reader;
    private final Path dir;
    private final String prefix;
    private final String suffix;
    private long nanos;
    private long batches;

    Timed(PartitionReader<ColumnarBatch> reader, Path dir, String prefix, String suffix) {
      this.reader = reader;
      this.dir = dir;
      this.prefix = prefix;
      this.suffix = suffix;
    }

    @Override
    public boolean next() throws IOException {
      long t0 = System.nanoTime();
      try {
        boolean more = reader.next();
        batches += more ? 1 : 0;
        return more;
      } finally {
        nanos += System.nanoTime() - t0;
      }
    }

    @Override
    public ColumnarBatch get() {
      long t0 = System.nanoTime();
      try {
        return reader.get();
      } finally {
        nanos += System.nanoTime() - t0;
      }
    }

    @Override
    public void close() throws IOException {
      reader.close();
      append(String.format(Locale.ROOT, "%s,%d,%.3f,%s", prefix, batches, nanos / 1e6, suffix));
    }

    private void append(String line) {
      try {
        Files.createDirectories(dir);
        Path file = dir.resolve("scans-" + ProcessHandle.current().pid() + ".csv");
        synchronized (LOCK) {
          boolean fresh = !Files.exists(file);
          try (FileWriter out = new FileWriter(file.toFile(), StandardCharsets.UTF_8, true)) {
            if (fresh) {
              out.write(HEADER + "\n");
            }
            out.write(line + "\n");
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
