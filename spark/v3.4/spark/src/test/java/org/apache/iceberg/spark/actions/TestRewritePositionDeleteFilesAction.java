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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.expr;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupRewriteResult;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.Result;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.FourColumnRecord;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class TestRewritePositionDeleteFilesAction extends SparkCatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  private static final Map<String, String> CATALOG_PROPS =
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "cache-enabled", "false");

  private static final int SCALE = 4000;
  private static final int DELETES_SCALE = 1000;

  @Parameterized.Parameters(
      name =
          "formatVersion = {0}, catalogName = {1}, implementation = {2}, config = {3}, fileFormat = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET
      }
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final FileFormat format;

  public TestRewritePositionDeleteFilesAction(
      String catalogName, String implementation, Map<String, String> config, FileFormat format) {
    super(catalogName, implementation, config);
    this.format = format;
  }

  @After
  public void cleanup() {
    validationCatalog.dropTable(TableIdentifier.of("default", TABLE_NAME));
  }

  @Test
  public void testEmptyTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME), SCHEMA, spec, tableProperties());

    Result result = SparkActions.get(spark).rewritePositionDeletes(table).execute();
    Assert.assertEquals("No rewritten delete files", 0, result.rewrittenDeleteFilesCount());
    Assert.assertEquals("No added delete files", 0, result.addedDeleteFilesCount());
  }

  @Test
  public void testFileGranularity() throws Exception {
    checkDeleteGranularity(DeleteGranularity.FILE);
  }

  @Test
  public void testPartitionGranularity() throws Exception {
    checkDeleteGranularity(DeleteGranularity.PARTITION);
  }

  private void checkDeleteGranularity(DeleteGranularity deleteGranularity) throws Exception {
    Table table = createTableUnpartitioned(2, SCALE);

    table
        .updateProperties()
        .set(TableProperties.DELETE_GRANULARITY, deleteGranularity.toString())
        .commit();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    assertThat(dataFiles).hasSize(2);

    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);

    List<DeleteFile> deleteFiles = deleteFiles(table);
    assertThat(deleteFiles).hasSize(2);

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    int expectedDeleteFilesCount = deleteGranularity == DeleteGranularity.FILE ? 2 : 1;
    assertThat(result.addedDeleteFilesCount()).isEqualTo(expectedDeleteFilesCount);
  }

  @Test
  public void testUnpartitioned() throws Exception {
    Table table = createTableUnpartitioned(2, SCALE);
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(2, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(2, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(2000, expectedRecords.size());
    Assert.assertEquals(2000, expectedDeletes.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Expected 1 new delete file", 1, newDeleteFiles.size());
    assertLocallySorted(newDeleteFiles);
    assertNotContains(deleteFiles, newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 1);
    checkSequenceNumbers(table, deleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRewriteAll() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 4 delete files", 4, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);
    checkSequenceNumbers(table, deleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRewriteFilter() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);
    table.refresh();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(8, deleteFiles.size());

    table.refresh();
    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size()); // 16000 data - 4000 delete rows
    Assert.assertEquals(4000, expectedDeletes.size());

    Expression filter =
        Expressions.and(
            Expressions.greaterThan("c3", "0"), // should have no effect
            Expressions.or(Expressions.equal("c1", 1), Expressions.equal("c1", 2)));

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .filter(filter)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    List<DeleteFile> newDeleteFiles = except(deleteFiles(table), deleteFiles);
    Assert.assertEquals("Should have 4 delete files", 2, newDeleteFiles.size());

    List<DeleteFile> expectedRewrittenFiles =
        filterFiles(table, deleteFiles, ImmutableList.of(1), ImmutableList.of(2));
    assertLocallySorted(newDeleteFiles);
    checkResult(result, expectedRewrittenFiles, newDeleteFiles, 2);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRewriteToSmallerTarget() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(8, deleteFiles.size());

    long avgSize = size(deleteFiles) / deleteFiles.size();

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, String.valueOf(avgSize / 2))
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 8 new delete files", 8, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);
    checkSequenceNumbers(table, deleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRemoveDanglingDeletes() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(
        table,
        2,
        DELETES_SCALE,
        dataFiles,
        true /* Disable commit-time ManifestFilterManager removal of dangling deletes */);

    Assert.assertEquals(4, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 0 new delete files", 0, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);
    checkSequenceNumbers(table, deleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    Assert.assertEquals("Should be no new position deletes", 0, actualDeletes.size());
  }

  @Test
  public void testSomePartitionsDanglingDeletes() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    // Rewrite half the data files
    Expression filter = Expressions.or(Expressions.equal("c1", 0), Expressions.equal("c1", 1));
    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .filter(filter)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 2 new delete files", 2, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);
    checkSequenceNumbers(table, deleteFiles, newDeleteFiles);

    // As only half the files have been rewritten,
    // we expect to retain position deletes only for those not rewritten
    expectedDeletes =
        expectedDeletes.stream()
            .filter(
                r -> {
                  Object[] partition = (Object[]) r[3];
                  return partition[0] == (Integer) 2 || partition[0] == (Integer) 3;
                })
            .collect(Collectors.toList());

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRewriteFilterRemoveDangling() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);
    table.refresh();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles, true);
    Assert.assertEquals(4, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(8, deleteFiles.size());

    table.refresh();
    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size()); // 16000 data - 4000 delete rows
    Assert.assertEquals(4000, expectedDeletes.size());

    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    Expression filter = Expressions.or(Expressions.equal("c1", 0), Expressions.equal("c1", 1));
    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .filter(filter)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    List<DeleteFile> newDeleteFiles = except(deleteFiles(table), deleteFiles);
    Assert.assertEquals("Should have 2 new delete files", 0, newDeleteFiles.size());

    List<DeleteFile> expectedRewrittenFiles =
        filterFiles(table, deleteFiles, ImmutableList.of(0), ImmutableList.of(1));
    checkResult(result, expectedRewrittenFiles, newDeleteFiles, 2);

    List<Object[]> actualRecords = records(table);
    List<Object[]> allDeletes = deleteRecords(table);
    // Only non-compacted deletes remain
    List<Object[]> expectedDeletesFiltered =
        filterDeletes(expectedDeletes, ImmutableList.of(2), ImmutableList.of(3));
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletesFiltered, allDeletes);
  }

  @Test
  public void testPartitionEvolutionAdd() throws Exception {
    Table table = createTableUnpartitioned(2, SCALE);
    List<DataFile> unpartitionedDataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, unpartitionedDataFiles);
    Assert.assertEquals(2, unpartitionedDataFiles.size());

    List<DeleteFile> unpartitionedDeleteFiles = deleteFiles(table);
    Assert.assertEquals(2, unpartitionedDeleteFiles.size());

    List<Object[]> expectedUnpartitionedDeletes = deleteRecords(table);
    List<Object[]> expectedUnpartitionedRecords = records(table);
    Assert.assertEquals(2000, expectedUnpartitionedRecords.size());
    Assert.assertEquals(2000, expectedUnpartitionedDeletes.size());

    table.updateSpec().addField("c1").commit();
    writeRecords(table, 2, SCALE, 2);
    List<DataFile> partitionedDataFiles =
        except(TestHelpers.dataFiles(table), unpartitionedDataFiles);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, partitionedDataFiles);
    Assert.assertEquals(2, partitionedDataFiles.size());

    List<DeleteFile> partitionedDeleteFiles = except(deleteFiles(table), unpartitionedDeleteFiles);
    Assert.assertEquals(4, partitionedDeleteFiles.size());

    List<Object[]> expectedDeletes = deleteRecords(table);
    List<Object[]> expectedRecords = records(table);
    Assert.assertEquals(4000, expectedDeletes.size());
    Assert.assertEquals(8000, expectedRecords.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    List<DeleteFile> rewrittenDeleteFiles =
        Stream.concat(unpartitionedDeleteFiles.stream(), partitionedDeleteFiles.stream())
            .collect(Collectors.toList());
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 3 new delete files", 3, newDeleteFiles.size());
    assertNotContains(rewrittenDeleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, rewrittenDeleteFiles, newDeleteFiles, 3);
    checkSequenceNumbers(table, rewrittenDeleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testPartitionEvolutionRemove() throws Exception {
    Table table = createTablePartitioned(2, 2, SCALE);
    List<DataFile> dataFilesUnpartitioned = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFilesUnpartitioned);
    Assert.assertEquals(2, dataFilesUnpartitioned.size());

    List<DeleteFile> deleteFilesUnpartitioned = deleteFiles(table);
    Assert.assertEquals(4, deleteFilesUnpartitioned.size());

    table.updateSpec().removeField("c1").commit();

    writeRecords(table, 2, SCALE);
    List<DataFile> dataFilesPartitioned =
        except(TestHelpers.dataFiles(table), dataFilesUnpartitioned);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFilesPartitioned);
    Assert.assertEquals(2, dataFilesPartitioned.size());

    List<DeleteFile> deleteFilesPartitioned = except(deleteFiles(table), deleteFilesUnpartitioned);
    Assert.assertEquals(2, deleteFilesPartitioned.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(4000, expectedDeletes.size());
    Assert.assertEquals(8000, expectedRecords.size());

    List<DeleteFile> expectedRewritten = deleteFiles(table);
    Assert.assertEquals(6, expectedRewritten.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 3 new delete files", 3, newDeleteFiles.size());
    assertNotContains(expectedRewritten, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, expectedRewritten, newDeleteFiles, 3);
    checkSequenceNumbers(table, expectedRewritten, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testSchemaEvolution() throws Exception {
    Table table = createTablePartitioned(2, 2, SCALE);
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(2, dataFiles.size());

    List<DeleteFile> deleteFiles = deleteFiles(table);
    Assert.assertEquals(4, deleteFiles.size());

    table.updateSchema().addColumn("c4", Types.StringType.get()).commit();
    writeNewSchemaRecords(table, 2, SCALE, 2, 2);

    int newColId = table.schema().findField("c4").fieldId();
    List<DataFile> newSchemaDataFiles =
        TestHelpers.dataFiles(table).stream()
            .filter(f -> f.upperBounds().containsKey(newColId))
            .collect(Collectors.toList());
    writePosDeletesForFiles(table, 2, DELETES_SCALE, newSchemaDataFiles);

    List<DeleteFile> newSchemaDeleteFiles = except(deleteFiles(table), deleteFiles);
    Assert.assertEquals(4, newSchemaDeleteFiles.size());

    table.refresh();
    List<Object[]> expectedDeletes = deleteRecords(table);
    List<Object[]> expectedRecords = records(table);
    Assert.assertEquals(4000, expectedDeletes.size()); // 4 files * 1000 per file
    Assert.assertEquals(12000, expectedRecords.size()); // 4 * 4000 - 4000

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    List<DeleteFile> rewrittenDeleteFiles =
        Stream.concat(deleteFiles.stream(), newSchemaDeleteFiles.stream())
            .collect(Collectors.toList());
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 2 new delete files", 4, newDeleteFiles.size());
    assertNotContains(rewrittenDeleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, rewrittenDeleteFiles, newDeleteFiles, 4);
    checkSequenceNumbers(table, rewrittenDeleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteManyColumns() throws Exception {
    List<Types.NestedField> fields =
        Lists.newArrayList(Types.NestedField.optional(0, "id", Types.LongType.get()));
    List<Types.NestedField> additionalCols =
        IntStream.range(1, 1010)
            .mapToObj(i -> Types.NestedField.optional(i, "c" + i, Types.StringType.get()))
            .collect(Collectors.toList());
    fields.addAll(additionalCols);
    Schema schema = new Schema(fields);
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 2).build();
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME), schema, spec, tableProperties());

    Dataset<Row> df =
        spark
            .range(4)
            .withColumns(
                IntStream.range(1, 1010)
                    .boxed()
                    .collect(Collectors.toMap(i -> "c" + i, i -> expr("CAST(id as STRING)"))));
    StructType sparkSchema = spark.table(name(table)).schema();
    spark
        .createDataFrame(df.rdd(), sparkSchema)
        .coalesce(1)
        .write()
        .format("iceberg")
        .mode("append")
        .save(name(table));

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    writePosDeletesForFiles(table, 1, 1, dataFiles);
    assertThat(dataFiles).hasSize(2);

    List<DeleteFile> deleteFiles = deleteFiles(table);
    assertThat(deleteFiles).hasSize(2);

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    assertThat(expectedRecords).hasSize(2);
    assertThat(expectedDeletes).hasSize(2);

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    assertThat(newDeleteFiles).hasSize(2);
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 2);
    checkSequenceNumbers(table, deleteFiles, newDeleteFiles);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  private Table createTablePartitioned(int partitions, int files, int numRecords) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME), SCHEMA, spec, tableProperties());

    writeRecords(table, files, numRecords, partitions);
    return table;
  }

  private Table createTableUnpartitioned(int files, int numRecords) {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableProperties());

    writeRecords(table, files, numRecords);
    return table;
  }

  private Map<String, String> tableProperties() {
    return ImmutableMap.of(
        TableProperties.DEFAULT_WRITE_METRICS_MODE,
        "full",
        TableProperties.FORMAT_VERSION,
        "2",
        TableProperties.DEFAULT_FILE_FORMAT,
        format.toString());
  }

  private void writeRecords(Table table, int files, int numRecords) {
    writeRecords(table, files, numRecords, 1);
  }

  private void writeRecords(Table table, int files, int numRecords, int numPartitions) {
    writeRecordsWithPartitions(
        table,
        files,
        numRecords,
        IntStream.range(0, numPartitions).mapToObj(ImmutableList::of).collect(Collectors.toList()));
  }

  private void writeRecordsWithPartitions(
      Table table, int files, int numRecords, List<List<Integer>> partitions) {
    int partitionTypeSize = table.spec().partitionType().fields().size();
    Assert.assertTrue(
        "This method currently supports only two columns as partition columns",
        partitionTypeSize <= 2);
    BiFunction<Integer, List<Integer>, ThreeColumnRecord> recordFunction =
        (i, partValues) -> {
          switch (partitionTypeSize) {
            case (0):
              return new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i));
            case (1):
              return new ThreeColumnRecord(partValues.get(0), String.valueOf(i), String.valueOf(i));
            case (2):
              return new ThreeColumnRecord(
                  partValues.get(0), String.valueOf(partValues.get(1)), String.valueOf(i));
            default:
              throw new ValidationException(
                  "This method currently supports only two columns as partition columns");
          }
        };
    List<ThreeColumnRecord> records =
        partitions.stream()
            .flatMap(
                partition ->
                    IntStream.range(0, numRecords)
                        .mapToObj(i -> recordFunction.apply(i, partition)))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .repartition(files)
        .write()
        .format("iceberg")
        .mode("append")
        .save(name(table));
    table.refresh();
  }

  private void writeNewSchemaRecords(
      Table table, int files, int numRecords, int startingPartition, int partitions) {
    List<FourColumnRecord> records =
        IntStream.range(startingPartition, startingPartition + partitions)
            .boxed()
            .flatMap(
                partition ->
                    IntStream.range(0, numRecords)
                        .mapToObj(
                            i ->
                                new FourColumnRecord(
                                    partition,
                                    String.valueOf(i),
                                    String.valueOf(i),
                                    String.valueOf(i))))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, FourColumnRecord.class)
        .repartition(files)
        .write()
        .format("iceberg")
        .mode("append")
        .save(name(table));
  }

  private List<Object[]> records(Table table) {
    return rowsToJava(
        spark.read().format("iceberg").load(name(table)).sort("c1", "c2", "c3").collectAsList());
  }

  private List<Object[]> deleteRecords(Table table) {
    String[] additionalFields;
    // do not select delete_file_path for comparison
    // as delete files have been rewritten
    if (table.spec().isUnpartitioned()) {
      additionalFields = new String[] {"pos", "row"};
    } else {
      additionalFields = new String[] {"pos", "row", "partition", "spec_id"};
    }
    return rowsToJava(
        spark
            .read()
            .format("iceberg")
            .load(name(table) + ".position_deletes")
            .select("file_path", additionalFields)
            .sort("file_path", "pos")
            .collectAsList());
  }

  private void writePosDeletesForFiles(
      Table table, int deleteFilesPerPartition, int deletesPerDataFile, List<DataFile> files)
      throws IOException {
    writePosDeletesForFiles(table, deleteFilesPerPartition, deletesPerDataFile, files, false);
  }

  private void writePosDeletesForFiles(
      Table table,
      int deleteFilesPerPartition,
      int deletesPerDataFile,
      List<DataFile> files,
      boolean transactional)
      throws IOException {

    Map<StructLike, List<DataFile>> filesByPartition =
        files.stream().collect(Collectors.groupingBy(ContentFile::partition));
    List<DeleteFile> deleteFiles =
        Lists.newArrayListWithCapacity(deleteFilesPerPartition * filesByPartition.size());

    for (Map.Entry<StructLike, List<DataFile>> filesByPartitionEntry :
        filesByPartition.entrySet()) {

      StructLike partition = filesByPartitionEntry.getKey();
      List<DataFile> partitionFiles = filesByPartitionEntry.getValue();

      int deletesForPartition = partitionFiles.size() * deletesPerDataFile;
      Assert.assertEquals(
          "Number of delete files per partition should be "
              + "evenly divisible by requested deletes per data file times number of data files in this partition",
          0,
          deletesForPartition % deleteFilesPerPartition);
      int deleteFileSize = deletesForPartition / deleteFilesPerPartition;

      int counter = 0;
      List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
      for (DataFile partitionFile : partitionFiles) {
        for (int deletePos = 0; deletePos < deletesPerDataFile; deletePos++) {
          deletes.add(Pair.of(partitionFile.path(), (long) deletePos));
          counter++;
          if (counter == deleteFileSize) {
            // Dump to file and reset variables
            OutputFile output = Files.localOutput(temp.newFile());
            deleteFiles.add(FileHelpers.writeDeleteFile(table, output, partition, deletes).first());
            counter = 0;
            deletes.clear();
          }
        }
      }
    }

    if (transactional) {
      RowDelta rowDelta = table.newRowDelta();
      deleteFiles.forEach(rowDelta::addDeletes);
      rowDelta.commit();
    } else {
      deleteFiles.forEach(
          deleteFile -> {
            RowDelta rowDelta = table.newRowDelta();
            rowDelta.addDeletes(deleteFile);
            rowDelta.commit();
          });
    }
  }

  private List<DeleteFile> deleteFiles(Table table) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<ScanTask> tasks = deletesTable.newBatchScan().planFiles();
    return Lists.newArrayList(
        CloseableIterable.transform(tasks, t -> ((PositionDeletesScanTask) t).file()));
  }

  private <T extends ContentFile<?>> List<T> except(List<T> first, List<T> second) {
    Set<String> secondPaths =
        second.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
    return first.stream()
        .filter(f -> !secondPaths.contains(f.path().toString()))
        .collect(Collectors.toList());
  }

  private void assertNotContains(List<DeleteFile> original, List<DeleteFile> rewritten) {
    Set<String> originalPaths =
        original.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
    Set<String> rewrittenPaths =
        rewritten.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
    rewrittenPaths.retainAll(originalPaths);
    Assert.assertEquals(0, rewrittenPaths.size());
  }

  private void assertLocallySorted(List<DeleteFile> deleteFiles) {
    for (DeleteFile deleteFile : deleteFiles) {
      Dataset<Row> deletes =
          spark.read().format("iceberg").load("default." + TABLE_NAME + ".position_deletes");
      deletes.filter(deletes.col("delete_file_path").equalTo(deleteFile.path().toString()));
      List<Row> rows = deletes.collectAsList();
      Assert.assertFalse("Empty delete file found", rows.isEmpty());
      int lastPos = 0;
      String lastPath = "";
      for (Row row : rows) {
        String path = row.getAs("file_path");
        long pos = row.getAs("pos");
        if (path.compareTo(lastPath) < 0) {
          Assert.fail(String.format("File_path not sorted, Found %s after %s", path, lastPath));
        } else if (path.equals(lastPath)) {
          Assert.assertTrue("Pos not sorted", pos >= lastPos);
        }
      }
    }
  }

  private String name(Table table) {
    String[] splits = table.name().split("\\.");
    Assert.assertEquals(3, splits.length);
    return String.format("%s.%s", splits[1], splits[2]);
  }

  private long size(List<DeleteFile> deleteFiles) {
    return deleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();
  }

  private List<Object[]> filterDeletes(List<Object[]> deletes, List<?>... partitionValues) {
    Stream<Object[]> matches =
        deletes.stream()
            .filter(
                r -> {
                  Object[] partition = (Object[]) r[3];
                  return Arrays.stream(partitionValues)
                      .map(partitionValue -> match(partition, partitionValue))
                      .reduce((a, b) -> a || b)
                      .get();
                });
    return sorted(matches).collect(Collectors.toList());
  }

  private boolean match(Object[] partition, List<?> expectedPartition) {
    return IntStream.range(0, expectedPartition.size())
        .mapToObj(j -> partition[j] == expectedPartition.get(j))
        .reduce((a, b) -> a && b)
        .get();
  }

  private Stream<Object[]> sorted(Stream<Object[]> deletes) {
    return deletes.sorted(
        (a, b) -> {
          String aFilePath = (String) a[0];
          String bFilePath = (String) b[0];
          int filePathCompare = aFilePath.compareTo(bFilePath);
          if (filePathCompare != 0) {
            return filePathCompare;
          } else {
            long aPos = (long) a[1];
            long bPos = (long) b[1];
            return Long.compare(aPos, bPos);
          }
        });
  }

  private List<DeleteFile> filterFiles(
      Table table, List<DeleteFile> files, List<?>... partitionValues) {
    List<Types.StructType> partitionTypes =
        table.specs().values().stream()
            .map(PartitionSpec::partitionType)
            .collect(Collectors.toList());
    List<PartitionData> partitionDatas =
        Arrays.stream(partitionValues)
            .map(
                partitionValue -> {
                  Types.StructType thisType =
                      partitionTypes.stream()
                          .filter(f -> f.fields().size() == partitionValue.size())
                          .findFirst()
                          .get();
                  PartitionData partition = new PartitionData(thisType);
                  for (int i = 0; i < partitionValue.size(); i++) {
                    partition.set(i, partitionValue.get(i));
                  }
                  return partition;
                })
            .collect(Collectors.toList());

    return files.stream()
        .filter(f -> partitionDatas.stream().anyMatch(data -> f.partition().equals(data)))
        .collect(Collectors.toList());
  }

  private void checkResult(
      Result result,
      List<DeleteFile> rewrittenDeletes,
      List<DeleteFile> newDeletes,
      int expectedGroups) {
    Assert.assertEquals(
        "Expected rewritten delete file count does not match",
        rewrittenDeletes.size(),
        result.rewrittenDeleteFilesCount());
    Assert.assertEquals(
        "Expected new delete file count does not match",
        newDeletes.size(),
        result.addedDeleteFilesCount());
    Assert.assertEquals(
        "Expected rewritten delete byte count does not match",
        size(rewrittenDeletes),
        result.rewrittenBytesCount());
    Assert.assertEquals(
        "Expected new delete byte count does not match",
        size(newDeletes),
        result.addedBytesCount());

    Assert.assertEquals(
        "Expected rewrite group count does not match",
        expectedGroups,
        result.rewriteResults().size());
    Assert.assertEquals(
        "Expected rewritten delete file count in all groups to match",
        rewrittenDeletes.size(),
        result.rewriteResults().stream()
            .mapToInt(FileGroupRewriteResult::rewrittenDeleteFilesCount)
            .sum());
    Assert.assertEquals(
        "Expected added delete file count in all groups to match",
        newDeletes.size(),
        result.rewriteResults().stream()
            .mapToInt(FileGroupRewriteResult::addedDeleteFilesCount)
            .sum());
    Assert.assertEquals(
        "Expected rewritten delete bytes in all groups to match",
        size(rewrittenDeletes),
        result.rewriteResults().stream()
            .mapToLong(FileGroupRewriteResult::rewrittenBytesCount)
            .sum());
    Assert.assertEquals(
        "Expected added delete bytes in all groups to match",
        size(newDeletes),
        result.rewriteResults().stream().mapToLong(FileGroupRewriteResult::addedBytesCount).sum());
  }

  private void checkSequenceNumbers(
      Table table, List<DeleteFile> rewrittenDeletes, List<DeleteFile> addedDeletes) {
    StructLikeMap<List<DeleteFile>> rewrittenFilesPerPartition =
        groupPerPartition(table, rewrittenDeletes);
    StructLikeMap<List<DeleteFile>> addedFilesPerPartition = groupPerPartition(table, addedDeletes);
    for (StructLike partition : rewrittenFilesPerPartition.keySet()) {
      Long maxRewrittenSeq =
          rewrittenFilesPerPartition.get(partition).stream()
              .mapToLong(ContentFile::dataSequenceNumber)
              .max()
              .getAsLong();
      List<DeleteFile> addedPartitionFiles = addedFilesPerPartition.get(partition);
      if (addedPartitionFiles != null) {
        addedPartitionFiles.forEach(
            d ->
                Assert.assertEquals(
                    "Sequence number should be max of rewritten set",
                    d.dataSequenceNumber(),
                    maxRewrittenSeq));
      }
    }
  }

  private StructLikeMap<List<DeleteFile>> groupPerPartition(
      Table table, List<DeleteFile> deleteFiles) {
    StructLikeMap<List<DeleteFile>> result =
        StructLikeMap.create(Partitioning.partitionType(table));
    for (DeleteFile deleteFile : deleteFiles) {
      StructLike partition = deleteFile.partition();
      List<DeleteFile> partitionFiles = result.get(partition);
      if (partitionFiles == null) {
        partitionFiles = Lists.newArrayList();
      }
      partitionFiles.add(deleteFile);
      result.put(partition, partitionFiles);
    }
    return result;
  }
}
