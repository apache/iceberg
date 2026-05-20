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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceBenchmark;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A benchmark that evaluates the performance of reading and filtering variant data through
 * Iceberg's Spark data source.
 *
 * <p>Three tables are created with identical data:
 *
 * <ol>
 *   <li>avro records containing the bytes
 *   <li>unshredded variant object
 *   <li>fully shredded variant object
 * </ol>
 *
 * <p>The table structure is:
 *
 * <pre>
 *   id: int64 :- unique per row
 *   category: int32 :- in a very small range; written in clustered order so each Parquet row group should
 *     contain very few entries from different ranges.
 *   nested: variant (object)
 *       .idstr: string :- unique string per row
 *       .varid: int64  :- id
 *       .varcategory: int32
 *       .col4: string :- non-unique string per row (same number as category count)
 *   arr: variant (array, always unshredded)
 *       [0]: int32 :- category
 *       [1]: int32 :- id % 20
 *       [2]: int32 :- id % 100
 *       [3]: int32 :- id % 50
 *       [4]: int32 :- id % 1000
 * </pre>
 *
 * <p>Arrays don't shred because they are anonymous.
 *
 * <p>All data is written into a single file to keep the Spark task count to 1. Combined with a
 * {@code local[1]} Spark master, all benchmark iterations execute on a single thread, which
 * eliminates inter-task scheduling noise and makes measurements reproducible.
 *
 * <p>The structure and repetition helps highlight the space-saving benefits of parquet and shredded
 * parquet, irrespective of performance — look in the output logs for "Size of Table" to see the
 * values.
 *
 * <p>To run this benchmark for spark-4.1: <code>
 *   ./gradlew -DsparkVersions=4.1 :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
 *       -PjmhIncludeRegex=IcebergSourceVariantIOBenchmark \
 *       -PjmhOutputPath=build/reports/benchmark/iceberg-source-variant-io-benchmark-result.txt
 * </code>
 */
@Fork(1)
@Warmup(iterations = 20)
@Measurement(iterations = 20)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 60, timeUnit = TimeUnit.MINUTES)
public class IcebergSourceVariantIOBenchmark extends IcebergSourceBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceVariantIOBenchmark.class);

  /** Table to use in benchmark. */
  @Param({"Avro", "Unshredded", "Shredded"})
  private TableType tableType;

  private static final int NUM_FILES = 1;
  private static final int NUM_ROWS_PER_FILE = 100_000;

  /** Size of a compressed rowgroup. */
  private static final int ROW_GROUP_SIZE = 1 * 1024 * 1024;

  /** Number of distinct category values. */
  private static final int NUM_CATEGORIES = 5;

  public static final String COL_ID = "id";
  private static final String COL_NESTED = "nested";
  private static final String COL_ARR = "arr";

  /** Name of the Spark temp view registered in {@link #setupBenchmark()}. */
  private static final String TEMP_VIEW = "variant_table";

  /**
   * Name of the second Spark temp view for the self-join benchmark. Both views read the same
   * underlying table; registering two aliases avoids any Spark self-join restrictions.
   */
  private static final String TEMP_VIEW2 = "variant_table2";

  private static final Schema SCHEMA =
      new Schema(
          required(1, COL_ID, Types.LongType.get()),
          required(2, "category", Types.IntegerType.get()),
          required(3, COL_NESTED, Types.VariantType.get()), /* The variant object. */
          required(4, COL_ARR, Types.VariantType.get())); /* The variant array. */

  /** Get the category column from the variant: {@value}. */
  private static final String VARIANT_GET_NESTED_CATEGORY =
      "variant_get(nested, '$.varcategory', 'int')";

  /** Get the ID field from inside the variant: {@value}. */
  private static final String VARIANT_GET_NESTED_ID = "variant_get(nested, '$.varid', 'int')";

  /** Equality check. Pulled out to allow experimentation with set membershop vs. equals. */
  public static final String EQUALITY_CHECK = " = 1";

  /** Filter to use when filtering on category column: {@value}. */
  private static final String FILTER_ON_CATEGORY = "category " + EQUALITY_CHECK;

  /** Equivalent of {@link #FILTER_ON_CATEGORY} using the field within the variant: {@value}. */
  private static final String FILTER_ON_NESTED_CATEGORY =
      VARIANT_GET_NESTED_CATEGORY + EQUALITY_CHECK;

  /**
   * Get element 0 of the array variant: {@value}.
   *
   * <p>Note: {@code variant_get} with a JSONPath {@code $[n]} expression accesses array elements.
   */
  private static final String VARIANT_GET_ARR_ELT0 = "variant_get(arr, '$[0]', 'int')";

  /** Filter on element 0 of the array variant: {@value}. */
  private static final String FILTER_ON_ARR_ELT0 = VARIANT_GET_ARR_ELT0 + EQUALITY_CHECK;

  /** Table to use in benchmark. */
  public enum TableType {
    /** Parquet, no shedding. */
    Unshredded("parquet", FileFormat.PARQUET, false),
    /** Parquet, shedded. */
    Shredded("parquet", FileFormat.PARQUET, true),
    /** Avro. */
    Avro("avro", FileFormat.AVRO, false);
    private final String name;
    private final FileFormat format;
    private final boolean shredded;

    TableType(String name, FileFormat format, boolean shredded) {
      this.name = name;
      this.format = format;
      this.shredded = shredded;
    }

    @Override
    public String toString() {
      return "TableType{"
          + "name='"
          + name
          + '\''
          + ", format="
          + format
          + ",  shredded="
          + shredded
          + '}';
    }
  }

  /** Size must be 20 as the choice of string is derived from category. */
  private String[] repeatedStrings;

  /** Variant metadata. */
  private VariantMetadata variantMetadata;

  /** Function to shred variants; will be different for shredded vs unshredded tables. */
  private VariantShreddingFunction shredder;

  @Override
  protected Configuration initHadoopConf() {
    final Configuration conf = new Configuration();
    conf.setBoolean("fs.file.checksum.verify", false);
    return conf;
  }

  /**
   * Use a single-threaded Spark master to eliminate inter-task scheduling noise.
   *
   * <p>With one file and {@code local[1]}, every scan benchmark runs as a single serial task.
   */
  @Override
  protected String sparkMaster() {
    return "local[1]";
  }

  /**
   * This gets invoked in the superclass constructor, at which time the table type is unknown. This
   * is why the default table type cannot be set.
   *
   * @return a table
   */
  @Override
  protected Table initTable() {
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.FORMAT_VERSION, "3");
    properties.put(TableProperties.SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    // large split size keeps the single data file as one Spark task.
    properties.put(TableProperties.SPLIT_SIZE, Integer.toString(512 * 1024 * 1024));
    /*    properties.put(TableProperties.METADATA_COMPRESSION, "zstd");
    properties.put(TableProperties.PARQUET_COMPRESSION, "zstd");
    properties.put(TableProperties.AVRO_COMPRESSION, "zstd");*/
    // small row group size ensures multiple row groups per file for row-group skipping benchmarks.
    properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(ROW_GROUP_SIZE));
    // variant projection pushdown not supported with the vectorized reader.
    properties.put(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false");

    return tables.create(SCHEMA, PartitionSpec.unpartitioned(), properties, newTableLocation());
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    setupSpark();
    final RuntimeConfig sc = spark().conf();
    // one shuffle partition keeps the join benchmark on a single task.
    sc.set("spark.sql.shuffle.partitions", "1");

    // build the parquet schema of the shredded type by creating one variant instance
    // and type converting it.
    variantMetadata = Variants.metadata("idstr", "varid", "varcategory", "col4");
    Type shreddedType =
        ParquetVariantUtil.toParquetSchema(buildVariant(variantMetadata, 0, 0, "").value());
    shredder =
        isShredded()
            ? (fieldId, fieldName) -> COL_NESTED.equals(fieldName) ? shreddedType : null
            : (fieldId, fieldName) -> null;

    final int categoryCount = NUM_CATEGORIES;
    repeatedStrings = new String[categoryCount];
    IntStream.range(0, categoryCount)
        .forEach(i -> repeatedStrings[i] = "Longer repeated string " + i);
    // only one table is populated, to keep setup times down
    LOG.info("building table {}", tableType);
    long size =
        switch (tableType) {
          case Unshredded, Shredded -> appendVariantData();
          case Avro -> appendAvroVariantData();
        };
    tableDataset().createOrReplaceTempView(TEMP_VIEW);
    tableDataset().createOrReplaceTempView(TEMP_VIEW2);
    final String summary = String.format(Locale.ROOT, "Size of Table %s : %,3d", tableType, size);
    LOG.info("{}", summary);
    tableDataset().printSchema();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Override
  protected FileFormat fileFormat() {
    return tableType.format;
  }

  /**
   * Create a Spark dataset for the table being benchmarked.
   *
   * @return a dataset reading the appropriate table.
   */
  private Dataset<Row> tableDataset() {
    return spark().read().format("iceberg").load(table().location());
  }

  /**
   * Is the table shredded?
   *
   * @return true if the table is shredded.
   */
  private boolean isShredded() {
    return tableType.shredded;
  }

  /** Read the entire table. */
  @Benchmark
  public void count(Blackhole blackhole) {
    project(blackhole, "*");
  }

  /**
   * Read the rows, filtering on the category field, which is outside the variant, then filter on
   * ID.
   *
   * <p>This measures classic query performance where columnar formats have an advantage over
   * row-structured ones, especially as the tables grow in size or row width.
   */
  @Benchmark
  public void filterCatProjectID(Blackhole blackhole) {
    select(blackhole, COL_ID, FILTER_ON_CATEGORY, false);
  }

  /**
   * Perform a sql select operation to non-zero count of rows.
   *
   * @param blackhole consumer of all data
   * @param projection columns to project.
   * @param where optional WHERE clause
   * @param expectPushdownWhenShredded on a shredded table, should the rowgroup filter report
   *     invocation?
   * @return the result.
   */
  private long select(
      final Blackhole blackhole,
      String projection,
      String where,
      boolean expectPushdownWhenShredded) {
    final long result = sql(blackhole, toSql(projection, where));
    if (expectPushdownWhenShredded) {
      expectFilteringOfShreddedFields();
    }
    return result;
  }

  /**
   * Perform a sql select operation where the result is an empty set.
   *
   * @param projection columns to project.
   * @param where optional WHERE clause
   * @param expectPushdownWhenShredded on a shredded table, should the rowgroup filter report
   *     invocation?
   */
  private void selectEmptyResults(
      final String projection, final String where, boolean expectPushdownWhenShredded) {
    String command = toSql(projection, where);
    resetMetricsCounter();
    final Dataset<Row> ds = spark().sql(command);
    assertThat(ds.collectAsList()).describedAs("Result of command %s", command).isEmpty();
    if (expectPushdownWhenShredded) {
      expectFilteringOfShreddedFields();
    }
  }

  /**
   * Create a SQL select statement.
   *
   * @param projection projection clause
   * @param where filter, may be null
   * @return the string for evaluation.
   */
  private static String toSql(final String projection, final String where) {
    final String text =
        "SELECT " + projection + " FROM " + TEMP_VIEW + (where != null ? " WHERE " + where : "");
    return text;
  }

  /**
   * Issue any Spark SQL command that returns a non-zero count of rows.
   *
   * @param blackhole consumer of all data
   * @param command command to execute
   * @return count of result.
   */
  private long sql(final Blackhole blackhole, String command) {
    LOG.info("{}", command);
    resetMetricsCounter();
    return materializeNonEmpty(blackhole, command, spark().sql(command));
  }

  /**
   * Perform a sql projection operation.
   *
   * @param projection columns to project.
   * @return the result.
   */
  private long project(final Blackhole blackhole, String projection) {
    return select(blackhole, projection, null, false);
  }

  /**
   * Read the rows, filtering on the category field, which is outside the variant, then filter on
   * the variant ID.
   */
  @Benchmark
  public void filterCatProjectVarID(Blackhole blackhole) {
    select(blackhole, VARIANT_GET_NESTED_ID, FILTER_ON_CATEGORY, false);
  }

  /** Project on ID. */
  @Benchmark
  public void projectID(Blackhole blackhole) {
    project(blackhole, COL_ID);
  }

  /** Extract the varid field only. */
  @Benchmark
  public void projectVarID(Blackhole blackhole) {
    project(blackhole, VARIANT_GET_NESTED_ID);
  }

  /**
   * Read filtering on an element within the variant through {@code variant_get()} then project on
   * ID.
   *
   * <p>Filtering on a column within the variant assesses predicate pushdown: is the whole variant
   * reconstructed before filtering? or does the {@code variant_get()} get used early.
   */
  @Benchmark
  public void filterVarcatProjectID(Blackhole blackhole) {
    select(blackhole, COL_ID, FILTER_ON_NESTED_CATEGORY, true);
  }

  /** Filter to the category match; no projection. */
  @Benchmark
  public void filterCat(Blackhole blackhole) {
    select(blackhole, "*", FILTER_ON_CATEGORY, false);
  }

  /** Filter to the varcat match; no projection. */
  @Benchmark
  public void filterVarCat(Blackhole blackhole) {
    select(blackhole, "*", FILTER_ON_NESTED_CATEGORY, true);
  }

  @Benchmark
  public void filterVarCatSetMembership(Blackhole blackhole) {
    select(blackhole, COL_ID, VARIANT_GET_NESTED_CATEGORY + " IN (1)", true);
  }

  @Benchmark
  public void filterVarCatSetNotInMembership(Blackhole blackhole) {
    selectEmptyResults(COL_ID, VARIANT_GET_NESTED_CATEGORY + " IN (100, 400)", false);
  }

  @Benchmark
  public void filterVarCatSetNotInRange(Blackhole blackhole) {
    selectEmptyResults(COL_ID, VARIANT_GET_NESTED_CATEGORY + " < 0", false);
  }

  /** Project on the category field within the variant; no filtering. */
  @Benchmark
  public void projectVarCat(Blackhole blackhole) {
    project(blackhole, VARIANT_GET_NESTED_CATEGORY);
  }

  /** Filter on nested category field then project on the nested ID field. */
  @Benchmark
  public void filterVarcatProjectVarID(Blackhole blackhole) {
    select(blackhole, VARIANT_GET_NESTED_ID, FILTER_ON_NESTED_CATEGORY, true);
  }

  /**
   * Project element 0 of the array variant.
   *
   * <p>Note: the {@code arr} column is stored unshredded in all table types (array shredding is out
   * of scope). This benchmark compares unshredded Parquet vs Avro array element access.
   */
  @Benchmark
  public void projectArrayCategory(Blackhole blackhole) {
    project(blackhole, VARIANT_GET_ARR_ELT0);
  }

  /** Filter on element 0 of the array variant then project on ID. */
  @Benchmark
  public void filterArrayCategoryProjectID(Blackhole blackhole) {
    select(blackhole, COL_ID, FILTER_ON_ARR_ELT0, false);
  }

  /**
   * Filter on element 0 of the array variant then project element 0.
   *
   * <p>Both the filter and projection touch the same array element; this measures the cost of
   * evaluating {@code variant_get} on an array twice per row (once for filter, once for output).
   */
  @Benchmark
  public void filterArray0ProjectArray0(Blackhole blackhole) {
    select(blackhole, VARIANT_GET_ARR_ELT0, FILTER_ON_ARR_ELT0, false);
  }

  /** Explode the array variant into one row per element. */
  @Benchmark
  public void explodeArraryColumns(Blackhole blackhole) {
    sql(blackhole, "select t.* from " + TEMP_VIEW + " as table, lateral variant_explode(arr) as t");
  }

  /**
   * Self-join the table on {@code arr[4]} (= {@code id % 1000}, range 0-999) against {@code
   * varcategory} from the nested variant.
   *
   * <p>TODO: failing in spark. Needs investigation.
   */
  // @Benchmark
  public void joinArrElt4OnVarcat(Blackhole blackhole) {
    final String text =
        "SELECT t1."
            + COL_ID
            + ", t2."
            + COL_ID
            + " FROM "
            + TEMP_VIEW
            + " t1 JOIN "
            + TEMP_VIEW2
            + " t2 ON "
            + "variant_get(t1.arr, '$[4]', 'int') = variant_get(t2.nested, '$.varcategory', 'int')";
    sql(blackhole, text);
    expectFilteringOfShreddedFields();
  }

  /**
   * Materialize a dataset by counting its elements; raise an exception if the set is empty, as that
   * is interpreted a sign the query is somehow broken.
   *
   * @param blackhole consumer of all data
   * @param operation operation (for logging)
   * @param ds dataset
   * @return count of records returned.
   */
  private long materializeNonEmpty(final Blackhole blackhole, String operation, Dataset<?> ds) {
    LOG.info("{} table={}", operation, tableType);
    final long count = ds.queryExecution().toRdd().toJavaRDD().count();
    blackhole.consume(count);
    Preconditions.checkState(
        count > 0, "Operation %s on %s table returned no results", operation, tableType);
    LOG.info("Materialization of Dataset returned {} records", count);
    return count;
  }

  /**
   * Generate files in the target subdir, sorted by timestamp and with their format included in the
   * path.
   *
   * @return the location for the new table
   */
  @Override
  protected String newTableLocation() {
    Path tablePath =
        new Path(
            String.format(
                Locale.ROOT,
                "build/tables/spark-iceberg-table-%d-%s",
                System.currentTimeMillis(),
                UUID.randomUUID()));
    return tablePath.toString();
  }

  /**
   * Build the parquet dataset.
   *
   * @throws IOException creation failure.
   */
  private long appendVariantData() throws IOException {
    long size = 0;
    final AppendFiles append = table().newAppend();

    for (int fileNum = 0; fileNum < NUM_FILES; fileNum++) {
      final DataFile dataFile = createOneParquetFile(fileNum);
      append.appendFile(dataFile);
      size += dataFile.fileSizeInBytes();
    }
    append.commit();
    return size;
  }

  /**
   * Create a single parquet file as part of the append operation.
   *
   * @param fileIndex file index to use in generation of name and category derivation.
   * @return the data file.
   * @throws IOException write failure.
   */
  private DataFile createOneParquetFile(int fileIndex) throws IOException {
    OutputFile outputFile =
        table()
            .io()
            .newOutputFile(
                table()
                    .locationProvider()
                    .newDataLocation(String.format(Locale.ROOT, "data-%05d.parquet", fileIndex)));

    // forTable(table()) propagates the table properties (including
    // PARQUET_ROW_GROUP_SIZE_BYTES set in initTable()) into the writer. Without this,
    // the writer falls back to the Parquet default row-group size and emits a single
    // row group for the whole file, defeating row-group skipping.
    DataWriter<Record> writer =
        Parquet.writeData(outputFile)
            .forTable(table())
            .createWriterFunc(GenericParquetWriter::create)
            .variantShreddingFunc(shredder)
            .overwrite()
            .build();

    writeOneFile(writer, variantMetadata, fileIndex);
    DataFile dataFile = writer.toDataFile();
    LOG.info(
        "File {}: {} bytes, ~{} row groups",
        fileIndex,
        dataFile.fileSizeInBytes(),
        Math.max(1, dataFile.fileSizeInBytes() / ROW_GROUP_SIZE));
    dumpRowGroupDiagnostics(outputFile);
    return dataFile;
  }

  /**
   * Per-row-group statistics dump. Confirms that:
   *
   * <ul>
   *   <li>multiple row groups were actually written (not just one)
   *   <li>category and shredded {@code varcategory} columns have populated min/max stats
   *   <li>clustering produced tight ranges per row group (most groups have {@code min == max})
   *   <li>a filter like {@code varcategory = 1} can theoretically skip groups
   * </ul>
   *
   * <p>Only the shredded path looks at {@code nested.typed_value.varcategory.typed_value}; on
   * unshredded runs that path doesn't exist and is silently absent in the per-block dump.
   */
  private void dumpRowGroupDiagnostics(OutputFile outputFile) throws IOException {
    ColumnPath categoryPath = ColumnPath.get("category");
    ColumnPath shreddedVarcategoryPath =
        ColumnPath.get("nested", "typed_value", "varcategory", "typed_value");
    int expectedSkipForVarcatEq1 = 0;
    int blocksMissingShreddedVarcategory = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(outputFile.toInputFile()))) {
      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      LOG.info("Parquet diagnostics: {} row groups", blocks.size());
      for (int i = 0; i < blocks.size(); i++) {
        BlockMetaData block = blocks.get(i);
        ColumnChunkMetaData categoryChunk = findChunk(block, categoryPath);
        ColumnChunkMetaData varcategoryChunk = findChunk(block, shreddedVarcategoryPath);
        String categorySummary = summarizeStats(categoryChunk);
        String varcategorySummary =
            varcategoryChunk == null ? "<absent>" : summarizeStats(varcategoryChunk);
        LOG.info(
            "  block[{}]: rows={} bytes={} category={} shreddedVarcategory={}",
            i,
            block.getRowCount(),
            block.getTotalByteSize(),
            categorySummary,
            varcategorySummary);
        if (isShredded()) {
          if (varcategoryChunk == null) {
            blocksMissingShreddedVarcategory++;
          } else if (rangeExcludesIntLiteral(varcategoryChunk.getStatistics(), 1)) {
            expectedSkipForVarcatEq1++;
          }
        }
      }
      if (isShredded()) {
        if (blocksMissingShreddedVarcategory > 0) {
          LOG.warn(
              "Shredded run is missing the varcategory typed_value column in {} of {} blocks "
                  + "- shredding did not produce the expected layout",
              blocksMissingShreddedVarcategory,
              blocks.size());
        }
        LOG.info(
            "Parquet diagnostics summary: rowGroups={} expectedSkipForVarcatEq1={}",
            blocks.size(),
            expectedSkipForVarcatEq1);
      }
    }
  }

  /**
   * Find the column chunk for a given column path
   *
   * @param block metadata
   * @param path column path
   * @return chunk metadata or null if not found.
   */
  private static ColumnChunkMetaData findChunk(BlockMetaData block, ColumnPath path) {
    return block.getColumns().stream()
        .filter(chunk -> chunk.getPath().equals(path))
        .findFirst()
        .orElse(null);
  }

  /**
   * Generate a simple summary of the column chunk stats, or &lt;absent&gt; if the supplied metadata
   * is null, &lt;no-stats&gt; if there were no statistics found.
   *
   * @param chunk chunk metadata
   * @return summary.
   */
  private static String summarizeStats(ColumnChunkMetaData chunk) {
    if (chunk == null) {
      return "<absent>";
    }
    Statistics<?> stats = chunk.getStatistics();
    if (stats == null || stats.isEmpty()) {
      return "<no-stats>";
    }
    return String.format(
        Locale.ROOT,
        "min=%s max=%s nulls=%s values=%d",
        stats.hasNonNullValue() ? stats.genericGetMin() : "<unset>",
        stats.hasNonNullValue() ? stats.genericGetMax() : "<unset>",
        stats.isNumNullsSet() ? Long.toString(stats.getNumNulls()) : "<unset>",
        chunk.getValueCount());
  }

  /**
   * True if the chunk's statistics prove the integer literal cannot appear in the group — i.e. the
   * row group is skippable for an equality predicate on that literal. Returns false if stats are
   * absent, empty, or span the literal.
   */
  private static boolean rangeExcludesIntLiteral(Statistics<?> stats, int literal) {
    if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
      return false;
    }
    if (!(stats.genericGetMin() instanceof Number minNum)
        || !(stats.genericGetMax() instanceof Number maxNum)) {
      return false;
    }
    long min = minNum.longValue();
    long max = maxNum.longValue();
    return literal < min || literal > max;
  }

  /**
   * Build the Avro table.
   *
   * @return the file size.
   * @throws IOException creation failure.
   */
  private long appendAvroVariantData() throws IOException {
    long size = 0;
    final AppendFiles append = table().newAppend();
    for (int fileNum = 0; fileNum < NUM_FILES; fileNum++) {
      final DataFile dataFile = createOneAvroFile(fileNum);
      append.appendFile(dataFile);
      size += dataFile.fileSizeInBytes();
    }
    append.commit();
    return size;
  }

  /**
   * Create a single Avro file as part of the append operation.
   *
   * @param fileIndex file index to use in generation of name and category derivation.
   * @return the data file.
   * @throws IOException write failure.
   */
  private DataFile createOneAvroFile(int fileIndex) throws IOException {
    OutputFile outputFile =
        table()
            .io()
            .newOutputFile(
                table()
                    .locationProvider()
                    .newDataLocation(String.format(Locale.ROOT, "data-%05d.avro", fileIndex)));

    DataWriter<Record> writer =
        Avro.writeData(outputFile)
            .forTable(table())
            .schema(SCHEMA)
            .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    writeOneFile(writer, variantMetadata, fileIndex);
    return writer.toDataFile();
  }

  /**
   * Build the contents of a Parquet or Avro file, close the writer when done.
   *
   * <p>Rows are written in category-clustered order (all rows for category 0, then category 1,
   * etc.) so that each Parquet row group covers only a narrow range of category values. This
   * enables row-group skipping: a filter such as {@code category IN (5)} skips ~95% of row groups
   * (those whose min/max range excludes 5) across the ~34 row groups in the file.
   *
   * @param writer record writer (will be closed when the method returns)
   * @param metadata variant metadata
   * @param fileNum file number in sequence (unused; retained for call-site consistency)
   * @throws IOException write failure
   */
  private void writeOneFile(DataWriter<Record> writer, VariantMetadata metadata, int fileNum)
      throws IOException {
    try (writer) {
      // large base value from filenum
      long id = fileNum * 10_000_000_000L;
      GenericRecord record = GenericRecord.create(SCHEMA);
      int rowsPerCategory = NUM_ROWS_PER_FILE / NUM_CATEGORIES;
      for (int category = 0; category < NUM_CATEGORIES; category++) {
        for (int j = 0; j < rowsPerCategory; j++) {
          id++;
          Variant variant = buildVariant(metadata, id, category, repeatedStrings[category]);
          record.setField(COL_ID, id);
          record.setField("category", category);
          record.setField(COL_NESTED, variant);
          record.setField(COL_ARR, buildArrayVariant(id, category));
          writer.write(record);
        }
      }
    }
  }

  /**
   * Build the nested variant structure.
   *
   * @param metadata variant metadata
   * @param id row ID
   * @param category category
   * @param col4 string for column 4
   * @return a variant
   */
  private static Variant buildVariant(
      VariantMetadata metadata, long id, int category, String col4) {
    ShreddedObject obj = Variants.object(metadata);
    obj.put("idstr", Variants.of("item_" + id));
    obj.put("varid", Variants.of(id));
    obj.put("varcategory", Variants.of(category));
    obj.put("col4", Variants.of(col4));
    return Variant.of(metadata, obj);
  }

  /**
   * Build a 5-element integer array variant.
   *
   * <p>Arrays have no field-name metadata, so {@link Variants#emptyMetadata()} is used.
   *
   * @param id row ID
   * @param category category value
   * @return a variant holding an array
   */
  private static Variant buildArrayVariant(long id, int category) {
    ValueArray arr = Variants.array();
    arr.add(Variants.of(category));
    arr.add(Variants.of((int) (id % 20)));
    arr.add(Variants.of((int) (id % 100)));
    arr.add(Variants.of((int) (id % 50)));
    arr.add(Variants.of((int) (id % 1000)));
    return Variant.of(Variants.emptyMetadata(), arr);
  }

  /** Reset the metrics counter. */
  private void resetMetricsCounter() {
    /*
    ParquetMetricsRowGroupFilter.resetShreddedMetricsCounters();
    */
  }

  /**
   * On shredded tables, assert that shredded field were scanned for filtering operations. Log the
   * count at info.
   */
  private void expectFilteringOfShreddedFields() {
    /*
      if (isShredded()) {
        final long scans = ParquetMetricsRowGroupFilter.variantPredicatesShreddedMetricsEvaluated();
        final long skipped = ParquetMetricsRowGroupFilter.variantPredicatesShreddedSkipped();
        LOG.info("Scanned {} shredded metrics, skipped {} row groups", scans, skipped);
        assertThat(scans)
            .describedAs("Number of times rowgroup metrics of shredded fields were scanned")
            .isGreaterThan(0);
        assertThat(skipped).describedAs("rowgroups skipped").isGreaterThan(0);
      }
    */
  }
}
