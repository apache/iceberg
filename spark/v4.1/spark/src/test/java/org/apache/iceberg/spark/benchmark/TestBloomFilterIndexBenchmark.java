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
package org.apache.iceberg.spark.benchmark;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.BloomFilterIndexUtil;
import org.apache.iceberg.spark.actions.BuildBloomFilterIndexSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

/**
 * A local-only benchmark for the Puffin "bloom-filter-v1" file-skipping index.
 *
 * <p>Disabled by default. Run explicitly with:
 *
 * <pre>
 * ICEBERG_BLOOM_INDEX_BENCHMARK=true ./gradlew -DsparkVersions=4.1 \
 *   :iceberg-spark:iceberg-spark-4.1_2.13:test \
 *   --tests org.apache.iceberg.spark.benchmark.TestBloomFilterIndexBenchmark
 * </pre>
 *
 * <p>Optional tuning via system properties:
 *
 * <ul>
 *   <li>iceberg.benchmark.numFiles (default: 1000)
 *   <li>iceberg.benchmark.rowsPerFile (default: 2000)
 *   <li>iceberg.benchmark.warmupRuns (default: 3)
 *   <li>iceberg.benchmark.measuredRuns (default: 10)
 *   <li>iceberg.benchmark.numDays (default: 10) for the partitioned table
 * </ul>
 */
public class TestBloomFilterIndexBenchmark {

  @TempDir private Path tempDir;

  private static final String BLOOM_BLOB_TYPE = "bloom-filter-v1";
  private static final String PROP_DATA_FILE = "data-file";
  private static final String PROP_NUM_BITS = "num-bits";
  private static final String PROP_NUM_HASHES = "num-hashes";

  @Test
  public void runBloomIndexBenchmark() {
    boolean enabled =
        Boolean.parseBoolean(System.getenv().getOrDefault("ICEBERG_BLOOM_INDEX_BENCHMARK", "false"))
            || Boolean.getBoolean("iceberg.bloomIndexBenchmark");
    assumeTrue(
        enabled,
        "Benchmark disabled. Re-run with ICEBERG_BLOOM_INDEX_BENCHMARK=true (or -Diceberg.bloomIndexBenchmark=true)");

    BenchmarkConfig config = BenchmarkConfig.fromSystemProperties();

    // Keep needles inside the same lexical domain as the table values (hex strings),
    // otherwise min/max pruning can hide the bloom index benefit.
    String needle = randomHex64();
    String miss = randomHex64();
    while (miss.equals(needle)) {
      miss = randomHex64();
    }

    StringBuilder report = new StringBuilder();

    Path warehouse = tempDir.resolve("warehouse");
    SparkSession spark = newSpark(warehouse, config.shufflePartitions());

    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.default");

      log(report, "");
      log(report, "==== Iceberg Bloom Index Benchmark (local) ====");
      log(report, "numFiles=" + config.numFiles() + ", rowsPerFile=" + config.rowsPerFile());
      log(report, "warmupRuns=" + config.warmupRuns() + ", measuredRuns=" + config.measuredRuns());
      log(report, "needle=" + needle);
      log(report, "");

      for (DatasetProfile profile : DatasetProfile.profilesToRun()) {
        runDatasetProfile(spark, config, needle, miss, profile, report);
      }

    } finally {
      spark.stop();
    }

    writeReport(report);
  }

  private static String randomHex64() {
    byte[] bytes = new byte[32];
    new SecureRandom().nextBytes(bytes);
    StringBuilder sb = new StringBuilder(64);
    for (byte b : bytes) {
      sb.append(String.format(Locale.ROOT, "%02x", b));
    }
    return sb.toString();
  }

  private enum DatasetProfile {
    STRESS("stress", "Stress (min/max defeated)", true),
    REALISTIC("real", "Realistic (random high-cardinality)", false);

    private final String suffix;
    private final String displayName;
    private final boolean defeatMinMax;

    DatasetProfile(String suffix, String displayName, boolean defeatMinMax) {
      this.suffix = suffix;
      this.displayName = displayName;
      this.defeatMinMax = defeatMinMax;
    }

    String suffix() {
      return suffix;
    }

    String displayName() {
      return displayName;
    }

    boolean defeatMinMax() {
      return defeatMinMax;
    }

    static List<DatasetProfile> profilesToRun() {
      // Optional filter: ICEBERG_BENCHMARK_DATASETS=stress,real (default: both).
      String raw = System.getenv().getOrDefault("ICEBERG_BENCHMARK_DATASETS", "");
      if (raw == null || raw.trim().isEmpty()) {
        return List.of(STRESS, REALISTIC);
      }

      String lowered = raw.toLowerCase(Locale.ROOT);
      boolean wantStress = lowered.contains("stress");
      boolean wantReal = lowered.contains("real");

      if (wantStress && wantReal) {
        return List.of(STRESS, REALISTIC);
      } else if (wantStress) {
        return List.of(STRESS);
      } else if (wantReal) {
        return List.of(REALISTIC);
      }

      // Unknown value; fall back to both.
      return List.of(STRESS, REALISTIC);
    }
  }

  private static void log(StringBuilder report, String line) {
    System.out.println(line);
    report.append(line).append(System.lineSeparator());
  }

  private static void writeReport(StringBuilder report) {
    // Write under dev/ so it's easy to find and not ignored by tooling.
    Path reportPath = Paths.get("dev/bloom-index-benchmark.txt");
    try {
      Files.createDirectories(reportPath.getParent());
      Files.writeString(reportPath, report.toString());
      System.out.println();
      System.out.println("Benchmark report written to: " + reportPath.toAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write benchmark report", e);
    }
  }

  private static void logTableFileCount(
      StringBuilder report, SparkSession spark, String tableName, String label) {
    try {
      Table table = Spark3Util.loadIcebergTable(spark, tableName);
      int totalFiles = 0;
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask ignored : tasks) {
          totalFiles++;
        }
      }
      log(report, "Table files (" + label + "): " + tableName + " -> " + totalFiles);
    } catch (Exception e) {
      log(report, "Table files (" + label + "): " + tableName + " -> n/a (" + e + ")");
    }
  }

  private static void logBloomIndexArtifactStats(
      StringBuilder report, SparkSession spark, String tableName, String columnName, String label) {
    try {
      Table table = Spark3Util.loadIcebergTable(spark, tableName);
      Snapshot snapshot = table.currentSnapshot();
      if (snapshot == null) {
        log(report, "Index artifacts (" + label + "): no current snapshot");
        return;
      }

      org.apache.iceberg.types.Types.NestedField field = table.schema().findField(columnName);
      if (field == null) {
        log(report, "Index artifacts (" + label + "): column not found: " + columnName);
        return;
      }

      int fieldId = field.fieldId();
      List<StatisticsFile> statsFilesForSnapshot =
          statsFilesForSnapshot(table, snapshot.snapshotId());
      BloomIndexArtifactStats stats = scanBloomIndexBlobs(table, statsFilesForSnapshot, fieldId);

      log(
          report,
          String.format(
              Locale.ROOT,
              "Index artifacts (%s): statsFiles=%d statsBytes=%d bloomBlobs=%d bloomPayloadBytes=%d coveredDataFiles=%d blobsMissingRequiredProps=%d",
              label,
              stats.statsFileCount(),
              stats.statsFilesBytes(),
              stats.bloomBlobCount(),
              stats.bloomPayloadBytes(),
              stats.coveredDataFiles(),
              stats.bloomBlobsMissingRequiredProps()));

    } catch (Exception e) {
      log(report, "Index artifacts (" + label + "): n/a (" + e + ")");
    }
  }

  private static List<StatisticsFile> statsFilesForSnapshot(Table table, long snapshotId) {
    return table.statisticsFiles().stream()
        .filter(sf -> sf.snapshotId() == snapshotId)
        .collect(Collectors.toList());
  }

  private static BloomIndexArtifactStats scanBloomIndexBlobs(
      Table table, List<StatisticsFile> statsFilesForSnapshot, int fieldId) throws IOException {
    int statsFileCount = statsFilesForSnapshot.size();
    long statsFilesBytes =
        statsFilesForSnapshot.stream().mapToLong(StatisticsFile::fileSizeInBytes).sum();

    int bloomBlobCount = 0;
    long bloomPayloadBytes = 0L;
    int bloomBlobsMissingRequiredProps = 0;
    Set<String> dataFilesCovered = Sets.newHashSet();

    for (StatisticsFile stats : statsFilesForSnapshot) {
      InputFile inputFile = table.io().newInputFile(stats.path());
      try (PuffinReader reader =
          Puffin.read(inputFile).withFileSize(stats.fileSizeInBytes()).build()) {
        FileMetadata fileMetadata = reader.fileMetadata();
        for (BlobMetadata bm : fileMetadata.blobs()) {
          if (!isBloomBlobForField(bm, fieldId)) {
            continue;
          }

          bloomBlobCount++;
          bloomPayloadBytes += bm.length();

          Map<String, String> props = bm.properties();
          if (props == null) {
            bloomBlobsMissingRequiredProps++;
            continue;
          }

          String dataFile = props.get(PROP_DATA_FILE);
          if (dataFile != null) {
            dataFilesCovered.add(dataFile);
          }

          if (!hasRequiredBloomProps(props)) {
            bloomBlobsMissingRequiredProps++;
          }
        }
      }
    }

    return new BloomIndexArtifactStats(
        statsFileCount,
        statsFilesBytes,
        bloomBlobCount,
        bloomPayloadBytes,
        dataFilesCovered.size(),
        bloomBlobsMissingRequiredProps);
  }

  private static boolean isBloomBlobForField(BlobMetadata bm, int fieldId) {
    if (!BLOOM_BLOB_TYPE.equals(bm.type())) {
      return false;
    }

    List<Integer> fields = bm.inputFields();
    if (fields == null || fields.isEmpty()) {
      return false;
    }

    return fields.get(0) == fieldId;
  }

  private static boolean hasRequiredBloomProps(Map<String, String> props) {
    return props.get(PROP_NUM_BITS) != null && props.get(PROP_NUM_HASHES) != null;
  }

  private static class BloomIndexArtifactStats {
    private final int statsFileCount;
    private final long statsFilesBytes;
    private final int bloomBlobCount;
    private final long bloomPayloadBytes;
    private final int coveredDataFiles;
    private final int bloomBlobsMissingRequiredProps;

    private BloomIndexArtifactStats(
        int statsFileCount,
        long statsFilesBytes,
        int bloomBlobCount,
        long bloomPayloadBytes,
        int coveredDataFiles,
        int bloomBlobsMissingRequiredProps) {
      this.statsFileCount = statsFileCount;
      this.statsFilesBytes = statsFilesBytes;
      this.bloomBlobCount = bloomBlobCount;
      this.bloomPayloadBytes = bloomPayloadBytes;
      this.coveredDataFiles = coveredDataFiles;
      this.bloomBlobsMissingRequiredProps = bloomBlobsMissingRequiredProps;
    }

    private int statsFileCount() {
      return statsFileCount;
    }

    private long statsFilesBytes() {
      return statsFilesBytes;
    }

    private int bloomBlobCount() {
      return bloomBlobCount;
    }

    private long bloomPayloadBytes() {
      return bloomPayloadBytes;
    }

    private int coveredDataFiles() {
      return coveredDataFiles;
    }

    private int bloomBlobsMissingRequiredProps() {
      return bloomBlobsMissingRequiredProps;
    }
  }

  private static SparkSession newSpark(Path warehouse, int shufflePartitions) {
    return SparkSession.builder()
        .appName("IcebergBloomIndexBenchmark")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        // Use loopback to avoid artifact/classloader RPC issues in local tests.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.catalog.bench", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bench.type", "hadoop")
        .config("spark.sql.catalog.bench.warehouse", warehouse.toAbsolutePath().toString())
        .getOrCreate();
  }

  private static void createTable(
      SparkSession spark, String tableName, boolean partitioned, boolean parquetBloom) {
    spark.sql("DROP TABLE IF EXISTS " + tableName);

    String partitionClause = partitioned ? "PARTITIONED BY (day)" : "";

    StringBuilder props = new StringBuilder();
    props.append("'write.format.default'='parquet'");
    props.append(", 'write.distribution-mode'='none'");
    props.append(", 'write.spark.fanout.enabled'='true'");
    // Keep files small so we get many files even on local.
    props.append(", 'write.target-file-size-bytes'='1048576'");

    if (parquetBloom) {
      props.append(", 'write.parquet.bloom-filter-enabled.column.id'='true'");
      props.append(", 'write.parquet.bloom-filter-fpp.column.id'='0.01'");
    }

    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE %s (day string, id string, file_id int, payload string) USING iceberg %s "
                + "TBLPROPERTIES (%s)",
            tableName,
            partitionClause,
            props));
  }

  private static void runDatasetProfile(
      SparkSession spark,
      BenchmarkConfig config,
      String needle,
      String miss,
      DatasetProfile profile,
      StringBuilder report) {

    String suffix = profile.suffix();
    boolean defeatMinMax = profile.defeatMinMax();

    String baseTable = "bench.default.bloom_bench_base_" + suffix;
    String rowGroupTable = "bench.default.bloom_bench_rowgroup_" + suffix;
    String puffinTable = "bench.default.bloom_bench_puffin_" + suffix;
    String partitionedPuffinTable = "bench.default.bloom_bench_part_puffin_" + suffix;

    log(report, "==== Dataset: " + profile.displayName() + " ====");
    log(report, "defeatMinMax=" + defeatMinMax);
    if (defeatMinMax) {
      log(
          report,
          "Notes: Each data file contains id=min(0x00..00) and id=max(0xff..ff) rows so min/max stats cannot prune for id = <randomHex>.");
      log(
          report,
          "       The needle value is injected into exactly one file; the miss value is not present in any file.");
      log(
          report,
          "       This isolates the incremental benefit of Puffin bloom-filter-v1 file-level pruning vs baseline/row-group bloom.");
    } else {
      log(
          report,
          "Notes: High-cardinality random IDs (UUID/hash-like). For most rows: id = sha2(\"salt:file_id:row_id\", 256).");
      log(
          report,
          "       The needle value is injected into exactly one file; the miss value is not present in any file.");
    }

    // A) baseline: no parquet row-group bloom, no puffin bloom index
    createTable(spark, baseTable, false /* partitioned */, false /* parquetBloom */);
    writeData(spark, baseTable, config, false /* partitioned */, needle, defeatMinMax);
    logTableFileCount(report, spark, baseTable, "base-" + suffix);

    // B) row-group bloom only: enable Parquet bloom filters for 'id'
    createTable(spark, rowGroupTable, false /* partitioned */, true /* parquetBloom */);
    writeData(spark, rowGroupTable, config, false /* partitioned */, needle, defeatMinMax);
    logTableFileCount(report, spark, rowGroupTable, "rowgroup-" + suffix);

    // C) puffin bloom index only: build puffin bloom index on id
    createTable(spark, puffinTable, false /* partitioned */, false /* parquetBloom */);
    writeData(spark, puffinTable, config, false /* partitioned */, needle, defeatMinMax);
    logTableFileCount(report, spark, puffinTable, "puffin-before-index-" + suffix);
    buildPuffinBloomIndex(spark, puffinTable, "id");
    logBloomIndexArtifactStats(report, spark, puffinTable, "id", "puffin-index-" + suffix);

    // D) partitioned + puffin bloom index: demonstrate benefit within a pruned partition
    createTable(spark, partitionedPuffinTable, true /* partitioned */, false /* parquetBloom */);
    writeData(spark, partitionedPuffinTable, config, true /* partitioned */, needle, defeatMinMax);
    logTableFileCount(
        report, spark, partitionedPuffinTable, "partitioned-puffin-before-index-" + suffix);
    buildPuffinBloomIndex(spark, partitionedPuffinTable, "id");
    logBloomIndexArtifactStats(
        report, spark, partitionedPuffinTable, "id", "partitioned-puffin-index-" + suffix);

    log(report, "");

    // Unpartitioned: point predicate
    benchmarkScenario(
        spark,
        "A) baseline (no row-group bloom, no puffin index)",
        baseTable,
        "SELECT count(*) AS c FROM %s WHERE id = '%s'",
        needle,
        miss,
        config,
        report);

    benchmarkScenario(
        spark,
        "B) row-group bloom only (Parquet row-group bloom on id)",
        rowGroupTable,
        "SELECT count(*) AS c FROM %s WHERE id = '%s'",
        needle,
        miss,
        config,
        report);

    benchmarkScenario(
        spark,
        "C) puffin bloom index only (file-skipping bloom-filter-v1 on id)",
        puffinTable,
        "SELECT count(*) AS c FROM %s WHERE id = '%s'",
        needle,
        miss,
        config,
        report);

    // Partitioned: prune by day then apply puffin bloom inside the remaining partition
    String needleDay = config.needleDay();
    String missDay = needleDay;
    benchmarkScenario(
        spark,
        "D) partitioned + puffin bloom index (day + id predicates)",
        partitionedPuffinTable,
        "SELECT count(*) AS c FROM %s WHERE day = '%s' AND id = '%s'",
        needleDay + "|" + needle,
        missDay + "|" + miss,
        config,
        report);
  }

  private static void writeData(
      SparkSession spark,
      String tableName,
      BenchmarkConfig config,
      boolean partitioned,
      String needle,
      boolean defeatMinMax) {
    int numFiles = config.numFiles();
    int rowsPerFile = config.rowsPerFile();
    int numDays = Math.max(1, config.numDays());

    // Choose a single file to contain the needle value.
    int needleFileId = Math.max(0, numFiles / 2);
    int needleRepeats = Math.min(50, Math.max(1, rowsPerFile / 20));

    long totalRows = (long) numFiles * (long) rowsPerFile;
    Dataset<Row> df = spark.range(totalRows).toDF();

    Column fileId = df.col("id").divide(rowsPerFile).cast("int").alias("file_id");
    Column posInFile =
        functions.pmod(df.col("id"), functions.lit(rowsPerFile)).cast("int").alias("pos_in_file");
    df = df.select(df.col("id").alias("row_id"), fileId, posInFile);

    Column dayCol;
    if (partitioned) {
      // Stable day string: 2026-01-01 + (file_id % numDays).
      dayCol =
          functions
              .date_format(
                  functions.date_add(
                      functions.lit("2026-01-01").cast("date"),
                      functions.pmod(df.col("file_id"), functions.lit(numDays))),
                  "yyyy-MM-dd")
              .alias("day");
    } else {
      dayCol = functions.lit("1970-01-01").alias("day");
    }

    // Deterministic high-cardinality IDs so per-file min/max is generally not selective.
    Column randomId =
        functions
            .sha2(
                functions.concat_ws(
                    ":", functions.lit("salt"), df.col("file_id"), df.col("row_id")),
                256)
            .alias("random_id");

    Column idCol;
    if (defeatMinMax) {
      String minId = "0000000000000000000000000000000000000000000000000000000000000000";
      String maxId = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
      idCol =
          functions
              // Force identical min/max across files to defeat min/max pruning.
              .when(df.col("pos_in_file").equalTo(0), functions.lit(minId))
              .when(df.col("pos_in_file").equalTo(1), functions.lit(maxId))
              // Inject the needle into exactly one file.
              .when(
                  df.col("file_id")
                      .equalTo(needleFileId)
                      .and(df.col("pos_in_file").geq(2))
                      .and(df.col("pos_in_file").lt(needleRepeats + 2)),
                  functions.lit(needle))
              .otherwise(randomId)
              .alias("id");
    } else {
      idCol =
          functions
              // Inject the needle into exactly one file.
              .when(
                  df.col("file_id")
                      .equalTo(needleFileId)
                      .and(df.col("pos_in_file").lt(needleRepeats)),
                  functions.lit(needle))
              .otherwise(randomId)
              .alias("id");
    }

    Column payload =
        functions
            .sha2(functions.concat_ws("-", functions.lit("p"), df.col("row_id")), 256)
            .alias("payload");

    Dataset<Row> out = df.select(dayCol, idCol, df.col("file_id"), payload);

    // Encourage one output file per file_id (and per day when partitioned).
    Dataset<Row> repartitioned =
        partitioned
            ? out.repartition(numFiles, out.col("day"), out.col("file_id"))
            : out.repartition(numFiles, out.col("file_id"));

    try {
      repartitioned.writeTo(tableName).append();
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new RuntimeException("Table not found: " + tableName, e);
    }
  }

  private static void buildPuffinBloomIndex(
      SparkSession spark, String tableName, String columnName) {
    Table table;
    try {
      table = Spark3Util.loadIcebergTable(spark, tableName);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException
        | org.apache.spark.sql.catalyst.parser.ParseException e) {
      throw new RuntimeException("Failed to load Iceberg table: " + tableName, e);
    }
    BuildBloomFilterIndexSparkAction.Result result =
        SparkActions.get(spark).buildBloomFilterIndex(table).column(columnName).execute();
    table.refresh();
  }

  private static void benchmarkScenario(
      SparkSession spark,
      String label,
      String tableName,
      String queryTemplate,
      String needleValue,
      String missValue,
      BenchmarkConfig config,
      StringBuilder report) {

    log(report, "---- " + label + " ----");

    ScenarioResult needle =
        runQueryWorkload(spark, tableName, queryTemplate, needleValue, config, true /* hasDay */);
    ScenarioResult miss =
        runQueryWorkload(spark, tableName, queryTemplate, missValue, config, true /* hasDay */);

    log(report, "Needle: " + needle.summary());
    log(report, "Miss  : " + miss.summary());
    log(report, "");
  }

  private static ScenarioResult runQueryWorkload(
      SparkSession spark,
      String tableName,
      String queryTemplate,
      String value,
      BenchmarkConfig config,
      boolean allowCompoundValue) {

    String query;
    String dayFilter = null;
    String idFilter;
    if (allowCompoundValue && value.contains("|") && queryTemplate.contains("day =")) {
      String[] parts = value.split("\\|", 2);
      dayFilter = parts[0];
      idFilter = parts[1];
      query = String.format(Locale.ROOT, queryTemplate, tableName, dayFilter, idFilter);
    } else {
      idFilter = value;
      query = String.format(Locale.ROOT, queryTemplate, tableName, idFilter);
    }

    FileCounts fileCounts = estimateFileCounts(spark, tableName, dayFilter, idFilter);

    // Warmup
    for (int i = 0; i < config.warmupRuns(); i++) {
      spark.sql(query).collectAsList();
    }

    List<Long> durationsMs = Lists.newArrayList();
    List<Long> numFiles = Lists.newArrayList();
    List<Long> bytes = Lists.newArrayList();

    for (int i = 0; i < config.measuredRuns(); i++) {
      QueryMetrics queryMetrics = timedCollect(spark, query);
      durationsMs.add(queryMetrics.durationMs());
      numFiles.add(queryMetrics.numFiles());
      bytes.add(queryMetrics.bytesRead());
    }

    return new ScenarioResult(
        durationsMs, numFiles, bytes, fileCounts.plannedFiles(), fileCounts.afterBloomFiles());
  }

  private static FileCounts estimateFileCounts(
      SparkSession spark, String tableName, String dayFilter, String idFilter) {
    try {
      Table table = Spark3Util.loadIcebergTable(spark, tableName);
      org.apache.iceberg.TableScan scan = table.newScan().filter(Expressions.equal("id", idFilter));
      if (dayFilter != null) {
        scan = scan.filter(Expressions.equal("day", dayFilter));
      }

      List<FileScanTask> plannedTasks = Lists.newArrayList();
      try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
        for (FileScanTask t : tasks) {
          plannedTasks.add(t);
        }
      }

      List<FileScanTask> afterBloom =
          BloomFilterIndexUtil.pruneTasksWithBloomIndex(
              table, table.currentSnapshot(), () -> plannedTasks, "id", idFilter);

      return new FileCounts(plannedTasks.size(), afterBloom.size());

    } catch (Exception e) {
      // If anything goes wrong, don't fail the benchmark; just omit file counts.
      return new FileCounts(-1, -1);
    }
  }

  private static QueryMetrics timedCollect(SparkSession spark, String query) {
    Dataset<Row> ds = spark.sql(query);
    long startNs = System.nanoTime();
    ds.collectAsList();
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

    SparkPlan plan = ds.queryExecution().executedPlan();
    if (plan instanceof AdaptiveSparkPlanExec) {
      plan = ((AdaptiveSparkPlanExec) plan).executedPlan();
    }

    long numFiles = sumMetricByName(plan, "numFiles");
    long bytesRead = sumMetricsMatching(plan, "bytes");

    return new QueryMetrics(durationMs, numFiles, bytesRead);
  }

  private static long sumMetricByName(SparkPlan plan, String metricName) {
    long sum = 0L;
    Map<String, SQLMetric> metrics = JavaConverters.mapAsJavaMap(plan.metrics());
    SQLMetric metric = metrics.get(metricName);
    if (metric != null) {
      sum += metric.value();
    }

    scala.collection.Iterator<SparkPlan> it = plan.children().iterator();
    while (it.hasNext()) {
      sum += sumMetricByName(it.next(), metricName);
    }

    return sum;
  }

  private static long sumMetricsMatching(SparkPlan plan, String tokenLower) {
    long sum = 0L;
    Map<String, SQLMetric> metrics = JavaConverters.mapAsJavaMap(plan.metrics());
    for (Map.Entry<String, SQLMetric> entry : metrics.entrySet()) {
      String key = entry.getKey();
      if (key != null && key.toLowerCase(Locale.ROOT).contains(tokenLower)) {
        sum += entry.getValue().value();
      }
    }

    scala.collection.Iterator<SparkPlan> it = plan.children().iterator();
    while (it.hasNext()) {
      sum += sumMetricsMatching(it.next(), tokenLower);
    }

    return sum;
  }

  private record QueryMetrics(long durationMs, long numFiles, long bytesRead) {}

  private record ScenarioResult(
      List<Long> durationsMs,
      List<Long> numFiles,
      List<Long> bytesRead,
      int plannedFiles,
      int afterBloomFiles) {
    String summary() {
      long p50 = percentile(durationsMs, 0.50);
      long p95 = percentile(durationsMs, 0.95);
      long filesMedian = percentile(numFiles, 0.50);
      long bytesMedian = percentile(bytesRead, 0.50);
      String planned = plannedFiles >= 0 ? String.valueOf(plannedFiles) : "n/a";
      String afterBloom = afterBloomFiles >= 0 ? String.valueOf(afterBloomFiles) : "n/a";
      return String.format(
          Locale.ROOT,
          "latency_ms(p50=%d,p95=%d) scanMetricFiles(p50=%d) plannedFiles=%s afterBloom=%s bytesMetric(p50=%d)",
          p50,
          p95,
          filesMedian,
          planned,
          afterBloom,
          bytesMedian);
    }
  }

  private record FileCounts(int plannedFiles, int afterBloomFiles) {}

  private static long percentile(List<Long> values, double percentile) {
    if (values.isEmpty()) {
      return 0L;
    }

    List<Long> sorted = Lists.newArrayList(values);
    sorted.sort(Comparator.naturalOrder());

    int size = sorted.size();
    int idx = (int) Math.ceil(percentile * size) - 1;
    idx = Math.max(0, Math.min(size - 1, idx));
    return sorted.get(idx);
  }

  private record BenchmarkConfig(
      int numFiles, int rowsPerFile, int warmupRuns, int measuredRuns, int numDays) {

    static BenchmarkConfig fromSystemProperties() {
      return new BenchmarkConfig(
          intProp("ICEBERG_BENCHMARK_NUM_FILES", "iceberg.benchmark.numFiles", 1000),
          intProp("ICEBERG_BENCHMARK_ROWS_PER_FILE", "iceberg.benchmark.rowsPerFile", 2000),
          intProp("ICEBERG_BENCHMARK_WARMUP_RUNS", "iceberg.benchmark.warmupRuns", 3),
          intProp("ICEBERG_BENCHMARK_MEASURED_RUNS", "iceberg.benchmark.measuredRuns", 10),
          intProp("ICEBERG_BENCHMARK_NUM_DAYS", "iceberg.benchmark.numDays", 10));
    }

    int shufflePartitions() {
      // Use a stable upper bound to keep planning reasonable locally.
      return Math.max(8, Math.min(2000, numFiles));
    }

    String needleDay() {
      // Needle is injected into file_id=numFiles/2, which maps to day=(file_id % numDays).
      int dayOffset = Math.floorMod(Math.max(0, numFiles / 2), Math.max(1, numDays));
      // 2026-01-01 + offset days
      java.time.LocalDate base = java.time.LocalDate.of(2026, 1, 1);
      return base.plusDays(dayOffset).toString();
    }

    private static int intProp(String envKey, String sysPropKey, int defaultValue) {
      String envValue = System.getenv(envKey);
      if (envValue != null && !envValue.isEmpty()) {
        try {
          return Integer.parseInt(envValue);
        } catch (NumberFormatException ignored) {
          // fall back to sys prop/default
        }
      }

      String sysPropValue = System.getProperty(sysPropKey);
      if (sysPropValue == null || sysPropValue.isEmpty()) {
        return defaultValue;
      }

      try {
        return Integer.parseInt(sysPropValue);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
  }
}
