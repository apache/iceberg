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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetPageIndexPruningBenchmarkPoc {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestParquetPageIndexPruningBenchmarkPoc.class);

  private static final int BENCHMARK_RECORD_COUNT = 500_000;

  private static final int BENCHMARK_PAGE_ROW_LIMIT = 1_000;

  private static final int BENCHMARK_PAYLOAD_BYTES = 256;

  /*
   * This is intentionally not a formal JMH benchmark.
   *
   * Warmups reduce JVM/JIT noise and the median helps reduce the impact
   * of occasional scheduling, GC, and filesystem-cache outliers.
   */
  private static final int WARMUP_RUNS = 2;
  private static final int MEASUREMENT_RUNS = 5;

  private static final long FILTER_START = 250_000L;
  private static final long FILTER_END = 250_100L;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "group_id", Types.IntegerType.get()),
          Types.NestedField.optional(3, "payload", Types.StringType.get()));

  /*
   * Model the Spark-like path for:
   *
   *   SELECT payload
   *   FROM table
   *   WHERE id >= 250000 AND id < 250100
   *
   * The filter column is retained in the physical read schema because
   * page pruning does not perform final row-level predicate evaluation.
   */
  private static final Schema BENCHMARK_READ_SCHEMA =
      new Schema(SCHEMA.findField("id"), SCHEMA.findField("payload"));

  @TempDir private File tempDir;

  /**
   * Counts the number of bytes successfully read from all streams opened through this InputFile.
   *
   * <p>This intentionally counts the extra footer/schema read caused by the current Page Index
   * POC's double-open behavior.
   */
  private static class CountingInputFile implements InputFile {
    private final InputFile delegate;
    private final AtomicLong bytesRead = new AtomicLong();

    private CountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return new CountingSeekableInputStream(delegate.newStream(), bytesRead);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }

    private long bytesRead() {
      return bytesRead.get();
    }
  }

  /** Counts bytes actually consumed from the underlying seekable stream. */
  private static class CountingSeekableInputStream extends SeekableInputStream {
    private final SeekableInputStream delegate;
    private final AtomicLong bytesRead;

    private CountingSeekableInputStream(SeekableInputStream delegate, AtomicLong bytesRead) {
      this.delegate = delegate;
      this.bytesRead = bytesRead;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int value = delegate.read();
      if (value >= 0) {
        bytesRead.incrementAndGet();
      }
      return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int read = delegate.read(buffer, offset, length);
      if (read > 0) {
        bytesRead.addAndGet(read);
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  /** Result from one measured reader execution. */
  private static class BenchmarkResult {

    private final String scenario;
    private final boolean pageIndexEnabled;
    private final long fileSizeBytes;
    private final long candidateRows;
    private final long bytesRead;
    private final long elapsedNanos;

    private BenchmarkResult(
        String scenario,
        boolean pageIndexEnabled,
        long fileSizeBytes,
        long candidateRows,
        long bytesRead,
        long elapsedNanos) {
      this.scenario = scenario;
      this.pageIndexEnabled = pageIndexEnabled;
      this.fileSizeBytes = fileSizeBytes;
      this.candidateRows = candidateRows;
      this.bytesRead = bytesRead;
      this.elapsedNanos = elapsedNanos;
    }

    private double elapsedMillis() {
      return elapsedNanos / 1_000_000.0;
    }

    private double bytesReadMiB() {
      return bytesRead / (1024.0 * 1024.0);
    }

    private double fileSizeMiB() {
      return fileSizeBytes / (1024.0 * 1024.0);
    }
  }

  /**
   * Writes one of the benchmark input layouts.
   *
   * <p>The three benchmark scenarios are:
   *
   * <ul>
   *   <li>sorted + ColumnIndex
   *   <li>random + ColumnIndex
   *   <li>sorted + no ColumnIndex
   * </ul>
   */
  private void writeBenchmarkFile(File parquetFile, boolean randomOrder, boolean columnIndexEnabled)
      throws IOException {
    OutputFile outputFile = Files.localOutput(parquetFile);
    long[] ids = new long[BENCHMARK_RECORD_COUNT];
    for (int i = 0; i < ids.length; i += 1) {
      ids[i] = i;
    }

    if (randomOrder) {
      Random random = new Random(34L);
      for (int i = ids.length - 1; i > 0; i -= 1) {
        int swapIndex = random.nextInt(i + 1);
        long temp = ids[i];
        ids[i] = ids[swapIndex];
        ids[swapIndex] = temp;
      }
    }

    String payloadPrefix = "x".repeat(BENCHMARK_PAYLOAD_BYTES);

    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(512 * 1024 * 1024))
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(1024 * 1024))
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, Integer.toString(BENCHMARK_PAGE_ROW_LIMIT))
            .set(TableProperties.PARQUET_COMPRESSION, "uncompressed")
            .set(
                TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + "id",
                Boolean.toString(columnIndexEnabled))
            .withDictionaryEncoding("id", false)
            .build()) {

      for (int physicalPosition = 0; physicalPosition < ids.length; physicalPosition += 1) {
        long id = ids[physicalPosition];
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        record.setField("group_id", (int) (id / 10_000L));
        record.setField("payload", payloadPrefix + "-" + id);
        appender.add(record);
      }
    }
  }

  /** Validates assumptions about the generated benchmark file before using it for measurements. */
  private void validateBenchmarkFile(File parquetFile, boolean expectColumnIndex)
      throws IOException {
    InputFile inputFile = Files.localInput(parquetFile);
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(inputFile), ParquetReadOptions.builder().build())) {
      assertThat(reader.getRowGroups()).as("Benchmark must use exactly one row group").hasSize(1);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      assertThat(rowGroup.getRowCount())
          .as("Benchmark row count")
          .isEqualTo(BENCHMARK_RECORD_COUNT);
      ColumnChunkMetaData idColumnChunk =
          rowGroup.getColumns().stream()
              .filter(column -> column.getPath().equals(ColumnPath.get("id")))
              .findFirst()
              .orElseThrow(
                  () -> new IllegalStateException("Could not find column chunk metadata for id"));
      Object columnIndex = reader.readColumnIndex(idColumnChunk);
      if (expectColumnIndex) {
        assertThat(columnIndex)
            .as("Expected id ColumnIndex in %s", parquetFile.getName())
            .isNotNull();
      } else {
        assertThat(columnIndex)
            .as("Expected no id ColumnIndex in %s", parquetFile.getName())
            .isNull();
      }

      LOG.info(
          "BENCHMARK_FILE file={}, fileSizeMiB={}, " + "rowGroups={}, rows={}, hasIdColumnIndex={}",
          parquetFile.getName(),
          parquetFile.length() / (1024.0 * 1024.0),
          reader.getRowGroups().size(),
          rowGroup.getRowCount(),
          columnIndex != null);
    }
  }

  private static Expression benchmarkFilter() {
    return Expressions.and(
        Expressions.greaterThanOrEqual("id", FILTER_START), Expressions.lessThan("id", FILTER_END));
  }

  /**
   * Runs one complete Iceberg custom-row-reader scan.
   *
   * <p>candidateRows means rows returned by the Parquet reader after page-level pruning. It does
   * not mean rows satisfying the final record-level predicate.
   */
  private BenchmarkResult runBenchmarkOnce(
      String scenario, File parquetFile, boolean pageIndexEnabled) throws IOException {
    CountingInputFile countingInputFile = new CountingInputFile(Files.localInput(parquetFile));
    Expression filter = benchmarkFilter();
    long candidateRows = 0L;
    long startNanos = System.nanoTime();

    if (pageIndexEnabled) {
      try (CloseableIterable<Record> rows =
          Parquet.read(countingInputFile)
              .project(BENCHMARK_READ_SCHEMA)
              .filter(filter)
              .createReaderFunc(GenericParquetReaders::buildReader)
              .enablePageIndexFilteringForPoc()
              .build()) {
        for (Record ignored : rows) {
          candidateRows += 1L;
        }
      }
    } else {
      try (CloseableIterable<Record> rows =
          Parquet.read(countingInputFile)
              .project(BENCHMARK_READ_SCHEMA)
              .filter(filter)
              .createReaderFunc(GenericParquetReaders::buildReader)
              .build()) {
        for (Record ignored : rows) {
          candidateRows += 1L;
        }
      }
    }

    long elapsedNanos = System.nanoTime() - startNanos;

    return new BenchmarkResult(
        scenario,
        pageIndexEnabled,
        parquetFile.length(),
        candidateRows,
        countingInputFile.bytesRead(),
        elapsedNanos);
  }

  /** Performs warmups and returns the measured run with the median elapsed time. */
  private BenchmarkResult runMedianBenchmark(
      String scenario, File parquetFile, boolean pageIndexEnabled) throws IOException {
    for (int i = 0; i < WARMUP_RUNS; i += 1) {
      runBenchmarkOnce(scenario, parquetFile, pageIndexEnabled);
    }
    List<BenchmarkResult> measurements = Lists.newArrayList();
    for (int i = 0; i < MEASUREMENT_RUNS; i += 1) {
      measurements.add(runBenchmarkOnce(scenario, parquetFile, pageIndexEnabled));
    }
    measurements.sort(Comparator.comparingLong(result -> result.elapsedNanos));
    return measurements.get(measurements.size() / 2);
  }

  private static double reductionPercent(long baseline, long optimized) {
    if (baseline == 0L) {
      return 0.0;
    }
    return 100.0 * (baseline - optimized) / baseline;
  }

  private static void logComparison(BenchmarkResult off, BenchmarkResult on) {
    assertThat(off.scenario).isEqualTo(on.scenario);
    assertThat(off.pageIndexEnabled).isFalse();
    assertThat(on.pageIndexEnabled).isTrue();

    double candidateReduction = reductionPercent(off.candidateRows, on.candidateRows);
    double byteReduction = reductionPercent(off.bytesRead, on.bytesRead);
    double timeReduction = reductionPercent(off.elapsedNanos, on.elapsedNanos);

    LOG.info(
        "BENCHMARK_RESULT scenario={}, pageIndex=OFF, "
            + "fileSizeMiB={}, candidateRows={}, "
            + "bytesRead={}, bytesReadMiB={}, medianMs={}",
        off.scenario,
        off.fileSizeMiB(),
        off.candidateRows,
        off.bytesRead,
        off.bytesReadMiB(),
        off.elapsedMillis());

    LOG.info(
        "BENCHMARK_RESULT scenario={}, pageIndex=ON, "
            + "fileSizeMiB={}, candidateRows={}, "
            + "bytesRead={}, bytesReadMiB={}, medianMs={}",
        on.scenario,
        on.fileSizeMiB(),
        on.candidateRows,
        on.bytesRead,
        on.bytesReadMiB(),
        on.elapsedMillis());

    LOG.info(
        "BENCHMARK_COMPARISON scenario={}, "
            + "candidateReductionPct={}, "
            + "byteReductionPct={}, "
            + "timeReductionPct={}",
        on.scenario,
        candidateReduction,
        byteReduction,
        timeReduction);
  }

  /**
   * Best-case-ish workload for Page Index pruning.
   *
   * <p>The file is physically sorted by id, so page-level min/max ranges should be tight and the
   * narrow predicate should select only a small number of pages.
   */
  @Test
  public void benchmarkSortedData() throws IOException {
    File parquetFile = new File(tempDir, "benchmark-sorted-index.parquet");
    writeBenchmarkFile(parquetFile, false, true);
    validateBenchmarkFile(parquetFile, true);
    BenchmarkResult off = runMedianBenchmark("sorted", parquetFile, false);
    BenchmarkResult on = runMedianBenchmark("sorted", parquetFile, true);

    assertThat(off.candidateRows).isEqualTo(BENCHMARK_RECORD_COUNT);

    assertThat(on.candidateRows)
        .isGreaterThanOrEqualTo(FILTER_END - FILTER_START)
        .isLessThan(off.candidateRows);
    /*
     * This is the key I/O assertion for the initial POC.
     */
    assertThat(on.bytesRead)
        .as("Page Index should reduce physical reads for sorted data")
        .isLessThan(off.bytesRead);
    logComparison(off, on);
  }

  /**
   * Negative/control workload.
   *
   * <p>The same ids are randomly distributed across pages. Page-level min/max ranges will generally
   * be broad, so the ColumnIndex should have much less pruning power.
   */
  @Test
  public void benchmarkRandomData() throws IOException {
    File parquetFile = new File(tempDir, "benchmark-random-index.parquet");
    writeBenchmarkFile(parquetFile, true, true);
    validateBenchmarkFile(parquetFile, true);
    BenchmarkResult off = runMedianBenchmark("random", parquetFile, false);
    BenchmarkResult on = runMedianBenchmark("random", parquetFile, true);
    assertThat(off.candidateRows).isEqualTo(BENCHMARK_RECORD_COUNT);

    assertThat(on.candidateRows)
        .isGreaterThanOrEqualTo(FILTER_END - FILTER_START)
        .isLessThanOrEqualTo(BENCHMARK_RECORD_COUNT);

    logComparison(off, on);
  }

  /**
   * Compatibility/control workload for files that do not contain a usable ColumnIndex for the
   * predicate column.
   */
  @Test
  public void benchmarkFileWithoutColumnIndex() throws IOException {
    File parquetFile = new File(tempDir, "benchmark-sorted-no-index.parquet");
    writeBenchmarkFile(parquetFile, false, false);

    validateBenchmarkFile(parquetFile, false);
    BenchmarkResult off = runMedianBenchmark("sorted-no-index", parquetFile, false);
    BenchmarkResult on = runMedianBenchmark("sorted-no-index", parquetFile, true);
    assertThat(off.candidateRows).isEqualTo(BENCHMARK_RECORD_COUNT);
    assertThat(on.candidateRows).isEqualTo(BENCHMARK_RECORD_COUNT);
    logComparison(off, on);
  }
}
