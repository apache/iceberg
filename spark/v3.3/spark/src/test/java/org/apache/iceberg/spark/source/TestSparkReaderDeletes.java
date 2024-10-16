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
package org.apache.iceberg.spark.source;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.spark.source.SparkSQLExecutionHelper.lastExecutedMetricValue;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DeleteReadTests;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.spark.source.metrics.NumDeletes;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReaderDeletes extends DeleteReadTests {

  private static TestHiveMetastore metastore = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  @Parameter(index = 1)
  private boolean vectorized;

  @Parameters(name = "format = {0}, vectorized = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.PARQUET, false},
      new Object[] {FileFormat.PARQUET, true},
      new Object[] {FileFormat.ORC, false},
      new Object[] {FileFormat.AVRO, false}
    };
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.appStateStore.asyncTracking.enable", false)
            .config("spark.ui.liveUpdate.period", 0)
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterAll
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  @AfterEach
  @Override
  public void cleanup() throws IOException {
    super.cleanup();
    dropTable("test3");
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Table table = catalog.createTable(TableIdentifier.of("default", name), schema);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));
    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format.name()).commit();
    if (format.equals(FileFormat.PARQUET) || format.equals(FileFormat.ORC)) {
      String vectorizationEnabled =
          format.equals(FileFormat.PARQUET)
              ? TableProperties.PARQUET_VECTORIZATION_ENABLED
              : TableProperties.ORC_VECTORIZATION_ENABLED;
      String batchSize =
          format.equals(FileFormat.PARQUET)
              ? TableProperties.PARQUET_BATCH_SIZE
              : TableProperties.ORC_BATCH_SIZE;
      table.updateProperties().set(vectorizationEnabled, String.valueOf(vectorized)).commit();
      if (vectorized) {
        // split 7 records to two batches to cover more code paths
        table.updateProperties().set(batchSize, "4").commit();
      }
    }
    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  protected boolean countDeletes() {
    return true;
  }

  @Override
  protected long deleteCount() {
    return Long.parseLong(lastExecutedMetricValue(spark, NumDeletes.DISPLAY_STRING));
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) {
    return rowSet(name, table.schema().select(columns).asStruct(), columns);
  }

  public StructLikeSet rowSet(String name, Types.StructType projection, String... columns) {
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", name).toString())
            .selectExpr(columns);

    StructLikeSet set = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              set.add(rowWrapper.wrap(row));
            });

    return set;
  }

  @TestTemplate
  public void testEqualityDeleteWithFilter() throws IOException {
    String tableName = table.name().substring(table.name().lastIndexOf(".") + 1);
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    table.newRowDelta().addDeletes(eqDeletes).commit();

    Types.StructType projection = table.schema().select("*").asStruct();
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .filter("data = 'a'") // select a deleted row
            .selectExpr("*");

    StructLikeSet actual = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actual.add(rowWrapper.wrap(row));
            });

    assertThat(actual).as("Table should contain no rows").hasSize(0);
  }

  @TestTemplate
  public void testReadEqualityDeleteRows() throws IOException {
    Schema deleteSchema1 = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteSchema1);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d") // id = 89
            );

    Schema deleteSchema2 = table.schema().select("id");
    Record idDelete = GenericRecord.create(deleteSchema2);
    List<Record> idDeletes =
        Lists.newArrayList(
            idDelete.copy("id", 121), // id = 121
            idDelete.copy("id", 122) // id = 122
            );

    DeleteFile eqDelete1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteSchema1);

    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            idDeletes,
            deleteSchema2);

    table.newRowDelta().addDeletes(eqDelete1).addDeletes(eqDelete2).commit();

    StructLikeSet expectedRowSet = rowSetWithIds(29, 89, 121, 122);

    Types.StructType type = table.schema().asStruct();
    StructLikeSet actualRowSet = StructLikeSet.create(type);

    CloseableIterable<CombinedScanTask> tasks =
        TableScanUtil.planTasks(
            table.newScan().planFiles(),
            TableProperties.METADATA_SPLIT_SIZE_DEFAULT,
            TableProperties.SPLIT_LOOKBACK_DEFAULT,
            TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    for (CombinedScanTask task : tasks) {
      try (EqualityDeleteRowReader reader =
          new EqualityDeleteRowReader(task, table, null, table.schema(), false)) {
        while (reader.next()) {
          actualRowSet.add(
              new InternalRowWrapper(
                      SparkSchemaUtil.convert(table.schema()), table.schema().asStruct())
                  .wrap(reader.get().copy()));
        }
      }
    }

    assertThat(actualRowSet).as("should include 4 deleted row").hasSize(4);
    assertThat(actualRowSet).as("deleted row should be matched").isEqualTo(expectedRowSet);
  }

  @TestTemplate
  public void testPosDeletesAllRowsInBatch() throws IOException {
    // read.parquet.vectorization.batch-size is set to 4, so the 4 rows in the first batch are all
    // deleted.
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 0L), // id = 29
            Pair.of(dataFile.path(), 1L), // id = 43
            Pair.of(dataFile.path(), 2L), // id = 61
            Pair.of(dataFile.path(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 43, 61, 89);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testPosDeletesWithDeletedColumn() throws IOException {
    // read.parquet.vectorization.batch-size is set to 4, so the 4 rows in the first batch are all
    // deleted.
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 0L), // id = 29
            Pair.of(dataFile.path(), 1L), // id = 43
            Pair.of(dataFile.path(), 2L), // id = 61
            Pair.of(dataFile.path(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSet(29, 43, 61, 89);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testEqualityDeleteWithDeletedColumn() throws IOException {
    String tableName = table.name().substring(table.name().lastIndexOf(".") + 1);
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    table.newRowDelta().addDeletes(eqDeletes).commit();

    StructLikeSet expected = expectedRowSet(29, 89, 122);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testMixedPosAndEqDeletesWithDeletedColumn() throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            dataSchema);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 3L), // id = 89
            Pair.of(dataFile.path(), 5L) // id = 121
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table
        .newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSet(29, 89, 121, 122);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testFilterOnDeletedMetadataColumn() throws IOException {
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 0L), // id = 29
            Pair.of(dataFile.path(), 1L), // id = 43
            Pair.of(dataFile.path(), 2L), // id = 61
            Pair.of(dataFile.path(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSetWithNonDeletesOnly(29, 43, 61, 89);

    // get non-deleted rows
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .select("id", "data", "_deleted")
            .filter("_deleted = false");

    Types.StructType projection = PROJECTION_SCHEMA.asStruct();
    StructLikeSet actual = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actual.add(rowWrapper.wrap(row));
            });

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);

    StructLikeSet expectedDeleted = expectedRowSetWithDeletesOnly(29, 43, 61, 89);

    // get deleted rows
    df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .select("id", "data", "_deleted")
            .filter("_deleted = true");

    StructLikeSet actualDeleted = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actualDeleted.add(rowWrapper.wrap(row));
            });

    assertThat(actualDeleted).as("Table should contain expected row").isEqualTo(expectedDeleted);
  }

  @TestTemplate
  public void testIsDeletedColumnWithoutDeleteFile() {
    StructLikeSet expected = expectedRowSet();
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");
    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(0L);
  }

  @TestTemplate
  public void testPosDeletesOnParquetFileWithMultipleRowGroups() throws IOException {
    assumeThat(format).isEqualTo("parquet");

    String tblName = "test3";
    Table tbl = createTable(tblName, SCHEMA, PartitionSpec.unpartitioned());

    List<Path> fileSplits = Lists.newArrayList();
    StructType sparkSchema = SparkSchemaUtil.convert(SCHEMA);
    Configuration conf = new Configuration();
    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();
    Path testFilePath = new Path(testFile.getAbsolutePath());

    // Write a Parquet file with more than one row group
    ParquetFileWriter parquetFileWriter =
        new ParquetFileWriter(conf, ParquetSchemaUtil.convert(SCHEMA, "test3Schema"), testFilePath);
    parquetFileWriter.start();
    for (int i = 0; i < 2; i += 1) {
      File split = File.createTempFile("junit", null, temp.toFile());
      assertThat(split.delete()).as("Delete should succeed").isTrue();
      Path splitPath = new Path(split.getAbsolutePath());
      fileSplits.add(splitPath);
      try (FileAppender<InternalRow> writer =
          Parquet.write(Files.localOutput(split))
              .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(sparkSchema, msgType))
              .schema(SCHEMA)
              .overwrite()
              .build()) {
        Iterable<InternalRow> records = RandomData.generateSpark(SCHEMA, 100, 34 * i + 37);
        writer.addAll(records);
      }
      parquetFileWriter.appendFile(
          org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(splitPath, conf));
    }
    parquetFileWriter.end(
        ParquetFileWriter.mergeMetadataFiles(fileSplits, conf)
            .getFileMetaData()
            .getKeyValueMetaData());

    // Add the file to the table
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(org.apache.iceberg.hadoop.HadoopInputFile.fromPath(testFilePath, conf))
            .withFormat("parquet")
            .withRecordCount(200)
            .build();
    tbl.newAppend().appendFile(dataFile).commit();

    // Add positional deletes to the table
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 97L),
            Pair.of(dataFile.path(), 98L),
            Pair.of(dataFile.path(), 99L),
            Pair.of(dataFile.path(), 101L),
            Pair.of(dataFile.path(), 103L),
            Pair.of(dataFile.path(), 107L),
            Pair.of(dataFile.path(), 109L));
    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes);
    tbl.newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    assertThat(rowSet(tblName, tbl, "*")).hasSize(193);
  }

  private static final Schema PROJECTION_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          MetadataColumns.IS_DELETED);

  private static StructLikeSet expectedRowSet(int... idsToRemove) {
    return expectedRowSet(false, false, idsToRemove);
  }

  private static StructLikeSet expectedRowSetWithDeletesOnly(int... idsToRemove) {
    return expectedRowSet(false, true, idsToRemove);
  }

  private static StructLikeSet expectedRowSetWithNonDeletesOnly(int... idsToRemove) {
    return expectedRowSet(true, false, idsToRemove);
  }

  private static StructLikeSet expectedRowSet(
      boolean removeDeleted, boolean removeNonDeleted, int... idsToRemove) {
    Set<Integer> deletedIds = Sets.newHashSet(ArrayUtil.toIntList(idsToRemove));
    List<Record> records = recordsWithDeletedColumn();
    // mark rows deleted
    records.forEach(
        record -> {
          if (deletedIds.contains(record.getField("id"))) {
            record.setField(MetadataColumns.IS_DELETED.name(), true);
          }
        });

    records.removeIf(record -> deletedIds.contains(record.getField("id")) && removeDeleted);
    records.removeIf(record -> !deletedIds.contains(record.getField("id")) && removeNonDeleted);

    StructLikeSet set = StructLikeSet.create(PROJECTION_SCHEMA.asStruct());
    records.forEach(
        record -> set.add(new InternalRecordWrapper(PROJECTION_SCHEMA.asStruct()).wrap(record)));

    return set;
  }

  @NotNull
  private static List recordsWithDeletedColumn() {
    List records = Lists.newArrayList();

    // records all use IDs that are in bucket id_bucket=0
    GenericRecord record = GenericRecord.create(PROJECTION_SCHEMA);
    records.add(record.copy("id", 29, "data", "a", "_deleted", false));
    records.add(record.copy("id", 43, "data", "b", "_deleted", false));
    records.add(record.copy("id", 61, "data", "c", "_deleted", false));
    records.add(record.copy("id", 89, "data", "d", "_deleted", false));
    records.add(record.copy("id", 100, "data", "e", "_deleted", false));
    records.add(record.copy("id", 121, "data", "f", "_deleted", false));
    records.add(record.copy("id", 122, "data", "g", "_deleted", false));
    return records;
  }
}
