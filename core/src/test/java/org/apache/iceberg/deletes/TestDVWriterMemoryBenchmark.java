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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Micro-benchmark comparing the retained heap of {@link BaseDVFileWriter} (one buffered position
 * index per touched data file until close) against {@link ClusteredDVFileWriter} (one live position
 * index, flushed per file) while writing deletes for N data files.
 *
 * <p>The measurement is taken just before {@code close()}: at that point the base writer holds
 * every file's bitmap while the clustered writer has already flushed all but the last one. Heap is
 * sampled after forced GC, so only strongly-reachable writer state is counted.
 *
 * <p>Results are printed and written to {@code build/dv-writer-memory-benchmark.txt}.
 */
public class TestDVWriterMemoryBenchmark {

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final int POSITIONS_PER_FILE = 2000;
  private static final long POSITION_STRIDE = 37;

  @TempDir private Path temp;

  @Test
  public void benchmarkBufferedStateBeforeClose() throws IOException {
    StringBuilder report = new StringBuilder();
    report.append(
        String.format(
            "positionsPerFile=%d stride=%d%n%12s %14s %18s %8s%n",
            POSITIONS_PER_FILE,
            POSITION_STRIDE,
            "files",
            "stockPeakMB",
            "clusteredPeakMB",
            "ratio"));

    for (int files : new int[] {1_000, 10_000, 50_000}) {
      long stockPeak = measurePeakHeap(files, out -> new BaseDVFileWriter(out, path -> null));
      long clusteredPeak =
          measurePeakHeap(files, out -> new ClusteredDVFileWriter(out, path -> null));

      report.append(
          String.format(
              "%12d %14.1f %18.1f %7.1fx%n",
              files,
              stockPeak / 1048576.0,
              clusteredPeak / 1048576.0,
              stockPeak / (double) Math.max(clusteredPeak, 1)));

      // the architectural claim: buffered state before close is bounded for the clustered
      // writer and proportional to the touched file count for the base writer
      assertThat(clusteredPeak).isLessThan(stockPeak);
    }

    System.out.println(report);
    java.nio.file.Files.write(
        java.nio.file.Paths.get("build", "dv-writer-memory-benchmark.txt"),
        report.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private long measurePeakHeap(int numFiles, Function<Supplier<OutputFile>, DVFileWriter> factory)
      throws IOException {
    Supplier<OutputFile> outputFile =
        () -> Files.localOutput(new File(temp.toFile(), "dv-" + UUID.randomUUID() + ".puffin"));

    long baseline = usedHeapAfterGc();

    DVFileWriter writer = factory.apply(outputFile);
    for (int file = 0; file < numFiles; file += 1) {
      String path = "s3://bucket/warehouse/db/table/data/partition=0/file-" + file + ".parquet";
      for (int pos = 0; pos < POSITIONS_PER_FILE; pos += 1) {
        writer.delete(path, pos * POSITION_STRIDE, SPEC, null);
      }
    }

    // peak buffered state: all deletes written, nothing closed yet
    long peak = usedHeapAfterGc() - baseline;

    writer.close();
    assertThat(writer.result().deleteFiles()).hasSize(numFiles);

    return peak;
  }

  private static long usedHeapAfterGc() {
    for (int i = 0; i < 3; i += 1) {
      System.gc();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }
}
