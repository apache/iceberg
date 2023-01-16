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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
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
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkReaderDeletes extends DeleteReadTests {

  private static TestHiveMetastore metastore = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;
  private final boolean vectorized;

  public TestSparkReaderDeletes(boolean vectorized) {
    this.vectorized = vectorized;
  }

  @Parameterized.Parameters(name = "vectorized = {0}")
  public static Object[][] parameters() {
    return new Object[][] {new Object[] {false}, new Object[] {true}};
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
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

  @AfterClass
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  @After
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
    table
        .updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, String.valueOf(vectorized))
        .commit();
    if (vectorized) {
      table
          .updateProperties()
          .set(
              TableProperties.PARQUET_BATCH_SIZE,
              "4") // split 7 records to two batches to cover more code paths
          .commit();
    }
    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) {
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", name).toString())
            .selectExpr(columns);

    Types.StructType projection = table.schema().select(columns).asStruct();
    StructLikeSet set = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              set.add(rowWrapper.wrap(row));
            });

    return set;
  }

  @Test
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
            Files.localOutput(temp.newFile()),
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

    Assert.assertEquals("Table should contain no rows", 0, actual.size());
  }

  @Test
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
            Files.localOutput(temp.newFile()),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteSchema1);

    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.newFile()),
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
          new EqualityDeleteRowReader(task, table, table.schema(), false)) {
        while (reader.next()) {
          actualRowSet.add(
              new InternalRowWrapper(SparkSchemaUtil.convert(table.schema()))
                  .wrap(reader.get().copy()));
        }
      }
    }

    Assert.assertEquals("should include 4 deleted row", 4, actualRowSet.size());
    Assert.assertEquals("deleted row should be matched", expectedRowSet, actualRowSet);
  }

  @Test
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
            table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deletes);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 43, 61, 89);
    StructLikeSet actual = rowSet(tableName, table, "*");

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testPosDeletesOnParquetFileWithMultipleRowGroups() throws IOException {
    String tblName = "test3";
    Table tbl = createTable(tblName, SCHEMA, PartitionSpec.unpartitioned());

    List<Path> fileSplits = Lists.newArrayList();
    StructType sparkSchema = SparkSchemaUtil.convert(SCHEMA);
    Configuration conf = new Configuration();
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());
    Path testFilePath = new Path(testFile.getAbsolutePath());

    // Write a Parquet file with more than one row group
    ParquetFileWriter parquetFileWriter =
        new ParquetFileWriter(conf, ParquetSchemaUtil.convert(SCHEMA, "test3Schema"), testFilePath);
    parquetFileWriter.start();
    for (int i = 0; i < 2; i += 1) {
      File split = temp.newFile();
      Assert.assertTrue("Delete should succeed", split.delete());
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
        FileHelpers.writeDeleteFile(table, Files.localOutput(temp.newFile()), deletes);
    tbl.newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    Assert.assertEquals(193, rowSet(tblName, tbl, "*").size());
  }
}
