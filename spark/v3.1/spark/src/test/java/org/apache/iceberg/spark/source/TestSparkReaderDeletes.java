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
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
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
  public static Object[] parameters() {
    return new Object[] {false, true};
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

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Table table = catalog.createTable(TableIdentifier.of("default", name), schema);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));
    if (vectorized) {
      table
          .updateProperties()
          .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
          .set(
              TableProperties.PARQUET_BATCH_SIZE,
              "4") // split 7 records to two batches to cover more code paths
          .commit();
    } else {
      table.updateProperties().set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false").commit();
    }
    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
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
  public void testPosDeletesWithDeletedColumn() throws IOException {
    Assume.assumeFalse(vectorized);

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

    StructLikeSet expected = expectedRowSet(29, 43, 61, 89);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    Assert.assertEquals("Table should contain expected row", expected, actual);
  }

  @Test
  public void testEqualityDeleteWithDeletedColumn() throws IOException {
    Assume.assumeFalse(vectorized);

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

    StructLikeSet expected = expectedRowSet(29, 89, 122);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    Assert.assertEquals("Table should contain expected row", expected, actual);
  }

  @Test
  public void testMixedPosAndEqDeletesWithDeletedColumn() throws IOException {
    Assume.assumeFalse(vectorized);

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
            Files.localOutput(temp.newFile()),
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
            table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deletes);

    table
        .newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSet(29, 89, 121, 122);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    Assert.assertEquals("Table should contain expected row", expected, actual);
  }

  @Test
  public void testFilterOnDeletedMetadataColumn() throws IOException {
    Assume.assumeFalse(vectorized);

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

    Assert.assertEquals("Table should contain expected row", expected, actual);

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

    Assert.assertEquals("Table should contain expected row", expectedDeleted, actualDeleted);
  }

  @Test
  public void testIsDeletedColumnWithoutDeleteFile() {
    Assume.assumeFalse(vectorized);

    StructLikeSet expected = expectedRowSet();
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");
    Assert.assertEquals("Table should contain expected row", expected, actual);
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
