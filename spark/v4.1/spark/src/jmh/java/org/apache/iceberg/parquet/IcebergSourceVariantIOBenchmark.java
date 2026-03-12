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

import java.io.IOException;
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
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
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
 *   category: int32 :- in range 0-19; written in clustered order so each Parquet row group covers
 *                      only a narrow category range (~1–2 row groups per category).
 *   nested: variant (object)
 *       .idstr: string :- unique string per row
 *       .varid: int64  :- id
 *       .varcategory: int32  :- category (0-19)
 *       .col4: string :- non-unique string per row (picked from 20 values based on category)
 *   arr: variant (array, always unshredded)
 *       [0]: int32 :- category (0-19)
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
@Warmup(iterations = 4)
@Measurement(iterations = 4)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class IcebergSourceVariantIOBenchmark extends IcebergSourceBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceVariantIOBenchmark.class);

  private static final int NUM_FILES = 1;
  private static final int NUM_ROWS_PER_FILE = 1_000_000;

  /**
   * At ~140 bytes/row (uncompressed), 4MB ≈ 29k rows/group. With 1M rows in one file clustered by
   * 20 categories (50k rows each), this gives ~1–2 row groups per category. Filter={@code category
   * IN (5)} skips ~32/34 row groups (95% skip rate).
   */
  private static final int ROW_GROUP_SIZE = 4 * 1024 * 1024; // 4 MB

  /** Number of distinct category values. */
  private static final int NUM_CATEGORIES = 20;

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

  /** Filter to use when filtering on category column: {@value}. */
  private static final String FILTER_ON_CATEGORY = "category = 5";

  /** Get the category column from the variant: {@value}. */
  private static final String VARIANT_GET_NESTED_CATEGORY =
      "variant_get(nested, '$.varcategory', 'int')";

  /** Get the ID field from inside the variant: {@value}. */
  private static final String VARIANT_GET_NESTED_ID = "variant_get(nested, '$.varid', 'int')";

  /**
   * Equality check.
   */
  public static final String EQUALITY_CHECK_5 = " = 5";

  /** Equivalent of {@link #FILTER_ON_CATEGORY} using the field within the variant: {@value}. */
  private static final String FILTER_ON_NESTED_CATEGORY =
      VARIANT_GET_NESTED_CATEGORY + EQUALITY_CHECK_5;

  /**
   * Get element 0 of the array variant; element 0 = category (range 0-19): {@value}.
   *
   * <p>Note: {@code variant_get} with a JSONPath {@code $[n]} expression accesses array elements.
   */
  private static final String VARIANT_GET_ARR_ELT0 = "variant_get(arr, '$[0]', 'int')";

  /**
   * Filter on element 0 of the array variant — same ~5% selectivity as {@link #FILTER_ON_CATEGORY}:
   * {@value}.
   */
  private static final String FILTER_ON_ARR_ELT0 = VARIANT_GET_ARR_ELT0 + EQUALITY_CHECK_5;

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

  /** Table to use in benchmark. */
  @Param({"Avro", "Unshredded", "Shredded"})
  private TableType tableType;

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
    // turn off compression to remove it as a factor.
    properties.put(TableProperties.METADATA_COMPRESSION, "none");
    properties.put(TableProperties.PARQUET_COMPRESSION, "none");
    properties.put(TableProperties.AVRO_COMPRESSION, "none");
    // small row group size ensures multiple row groups per file for row-group skipping benchmarks.
    properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(ROW_GROUP_SIZE));
    // variant projection pushdown not supported with the vectorized reader.
    properties.put(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false");

    return tables.create(SCHEMA, PartitionSpec.unpartitioned(), properties, newTableLocation());
  }

  @Setup
  public void setupBenchmark() throws IOException {
    setupSpark();
    // one shuffle partition keeps the join benchmark on a single task.
    spark().conf().set("spark.sql.shuffle.partitions", "1");

    // build the parquet schema of the shredded type by creating one variant instance
    // and type converting it.
    variantMetadata = Variants.metadata("idstr", "varid", "varcategory", "col4");
    Type shreddedType =
        ParquetVariantUtil.toParquetSchema(buildVariant(variantMetadata, 0, 0, "").value());
    shredder =
        tableType.shredded
            ? (fieldId, fieldName) -> COL_NESTED.equals(fieldName) ? shreddedType : null
            : (fieldId, fieldName) -> null;

    // algorithm for category produces a value 0..19
    final int categoryCount = 20;
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

  /** Read the entire table. */
  @Benchmark
  public void count() {
    project("*");
  }

  /**
   * Read the rows, filtering on the category field, which is outside the variant, then filter on
   * ID.
   *
   * <p>This measures classic query performance where columnar formats have an advantage over
   * row-structured ones, especially as the tables grow in size or row width.
   */
  @Benchmark
  public void filterCatProjectID() {
    select(COL_ID, FILTER_ON_CATEGORY);
  }

  /**
   * Perform a sql select operation.
   *
   * @param projection columns to project.
   * @param where optional WHERE clause
   * @return the result.
   */
  private long select(String projection, String where) {
    final String text =
        "SELECT " + projection + " FROM " + TEMP_VIEW + (where != null ? " WHERE " + where : "");
    return sql(text);
  }

  /**
   * Issue any Spark SQL command.
   *
   * @param command command to execute
   * @return count of result.
   */
  private long sql(String command) {
    LOG.info("{}", command);
    return materializeNonEmpty(command, spark().sql(command));
  }

  /**
   * Perform a sql projection operation.
   *
   * @param projection columns to project.
   * @return the result.
   */
  private long project(String projection) {
    return select(projection, null);
  }

  /**
   * Read the rows, filtering on the category field, which is outside the variant, then filter on
   * the variant ID.
   */
  @Benchmark
  public void filterCatProjectVarID() {
    select(VARIANT_GET_NESTED_ID, FILTER_ON_CATEGORY);
  }

  /** Project on ID. */
  @Benchmark
  public void projectID() {
    project(COL_ID);
  }

  /** Extract the varid field only. */
  @Benchmark
  public void projectVarID() {
    project(VARIANT_GET_NESTED_ID);
  }

  /**
   * Read filtering on an element within the variant through {@code variant_get()} then project on
   * ID.
   *
   * <p>Filtering on a column within the variant assesses predicate pushdown: is the whole variant
   * reconstructed before filtering? or does the {@code variant_get()} get used early.
   */
  @Benchmark
  public void filterVarcatProjectID() {
    select(COL_ID, FILTER_ON_NESTED_CATEGORY);
  }

  /** Filter to the category match; no projection. */
  @Benchmark
  public void filterCat() {
    select("*", FILTER_ON_CATEGORY);
  }

  /** Filter to the varcat match; no projection. */
  @Benchmark
  public void filterVarCat() {
    select("*", FILTER_ON_NESTED_CATEGORY);
  }

  @Benchmark
  public void filterVarCatSetMembership() {
    select(COL_ID, VARIANT_GET_NESTED_CATEGORY + " IN (5)");
  }

  @Benchmark
  public void filterVarCatSetNotInMembership() {
    select(COL_ID, VARIANT_GET_NESTED_CATEGORY + " IN (100, 400)");
  }

  @Benchmark
  public void filterVarCatSetNotInRange() {
    select(COL_ID, VARIANT_GET_NESTED_CATEGORY + " < 0");
  }

  /** Project on the category field within the variant; no filtering. */
  @Benchmark
  public void projectVarCat() {
    project(VARIANT_GET_NESTED_CATEGORY);
  }

  /** Filter on nested category field then project on the nested ID field. */
  @Benchmark
  public void filterVarcatProjectVarID() {
    select(VARIANT_GET_NESTED_ID, FILTER_ON_NESTED_CATEGORY);
  }

  /**
   * Materialize a dataset by counting its elements; raise an exception if the set is empty, as that
   * is interpreted a sign the query is somehow broken.
   *
   * @param operation operation (for logging)
   * @param ds dataset
   * @return count of records returned.
   */
  private long materializeNonEmpty(String operation, Dataset<?> ds) {
    LOG.info("{} table={}", operation, tableType);
    final long count = ds.count();
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

    DataWriter<Record> writer =
        Parquet.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .variantShreddingFunc(shredder)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    writeOneFile(writer, variantMetadata, fileIndex);
    DataFile dataFile = writer.toDataFile();
    LOG.info(
        "File {}: {} bytes, ~{} row groups",
        fileIndex,
        dataFile.fileSizeInBytes(),
        Math.max(1, dataFile.fileSizeInBytes() / ROW_GROUP_SIZE));
    return dataFile;
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
   * @param category category value (0-19)
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

  /**
   * Project element 0 of the array variant.
   *
   * <p>Note: the {@code arr} column is stored unshredded in all table types (array shredding is out
   * of scope). This benchmark compares unshredded Parquet vs Avro array element access.
   */
  @Benchmark
  public void projectArrayCategory() {
    project(VARIANT_GET_ARR_ELT0);
  }

  /**
   * Filter on element 0 of the array variant then project on ID.
   *
   * <p>Element 0 equals {@code category} (0-19), so this has the same ~5% selectivity as {@link
   * #filterCatProjectID()}, enabling direct comparison.
   */
  @Benchmark
  public void filterArraryCategoryProjectID() {
    select(COL_ID, FILTER_ON_ARR_ELT0);
  }

  /**
   * Filter on element 0 of the array variant then project element 0.
   *
   * <p>Both the filter and projection touch the same array element; this measures the cost of
   * evaluating {@code variant_get} on an array twice per row (once for filter, once for output).
   */
  @Benchmark
  public void filterArray0ProjectArray0() {
    select(VARIANT_GET_ARR_ELT0, FILTER_ON_ARR_ELT0);
  }

  /**
   * Explode the array variant into one row per element.
   *
   * <p>With 5 elements per row and 1M rows the result set is 5M rows. Uses Spark's TVF syntax:
   * {@code LATERAL variant_explode(arr) AS t}, which returns {@code (pos, key, value)} — {@code
   * key} is null for arrays.
   */
  @Benchmark
  public void explodeArraryColumns() {
    sql("select t.* from " + TEMP_VIEW + " as table, lateral variant_explode(arr) as t");
  }

  /**
   * Self-join the table on {@code arr[4]} (= {@code id % 1000}, range 0-999) against {@code
   * varcategory} (range 0-19) from the nested variant.
   *
   * <p>Because arr[4] spans 0-999 and varcategory spans 0-19, only ~2% of probe rows match, giving
   * a small result set. This benchmark measures the cost of extracting variant values on both sides
   * of a join condition.
   */
  @Benchmark
  public void joinArrElt4OnVarcat() {
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
    sql(text);
  }
}
