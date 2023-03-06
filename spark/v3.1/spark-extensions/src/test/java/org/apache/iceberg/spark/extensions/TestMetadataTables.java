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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataTables extends SparkExtensionsTestBase {

  public TestMetadataTables(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testFilesTableTimeTravelWithSchemaEvolution() throws Exception {
    // Create table and insert data
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> recordsA =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "a"));
    spark
        .createDataset(recordsA, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    table.updateSchema().addColumn("category", Types.StringType.get()).commit();

    List<Row> newRecords =
        Lists.newArrayList(RowFactory.create(3, "b", "c"), RowFactory.create(4, "b", "c"));

    StructType newSparkSchema =
        SparkSchemaUtil.convert(
            new Schema(
                optional(1, "id", Types.IntegerType.get()),
                optional(2, "data", Types.StringType.get()),
                optional(3, "category", Types.StringType.get())));

    spark.createDataFrame(newRecords, newSparkSchema).coalesce(1).writeTo(tableName).append();

    Long currentSnapshotId = table.currentSnapshot().snapshotId();

    Dataset<Row> actualFilesDs =
        spark
            .read()
            .format("iceberg")
            .option("snapshot-id", currentSnapshotId)
            .load(tableName + ".files")
            .orderBy("content");

    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();
    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    List<Record> expectedFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);

    Assert.assertEquals("actualFiles size should be 2", 2, actualFiles.size());

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));

    Assert.assertEquals(
        "expectedFiles and actualFiles size should be the same",
        actualFiles.size(),
        expectedFiles.size());
  }

  /**
   * Find matching manifest entries of an Iceberg table
   *
   * @param table iceberg table
   * @param expectedContent file content to populate on entries
   * @param entriesTableSchema schema of Manifest entries
   * @param manifestsToExplore manifests to explore of the table
   * @param partValue partition value that manifest entries must match, or null to skip filtering
   */
  private List<Record> expectedEntries(
      Table table,
      FileContent expectedContent,
      Schema entriesTableSchema,
      List<ManifestFile> manifestsToExplore,
      String partValue)
      throws IOException {
    List<Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : manifestsToExplore) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<Record> rows = Avro.read(in).project(entriesTableSchema).build()) {
        for (Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            Record file = (Record) record.get("data_file");
            if (partitionMatch(file, partValue)) {
              TestHelpers.asMetadataRecord(file, expectedContent);
              expected.add(file);
            }
          }
        }
      }
    }
    return expected;
  }

  private boolean partitionMatch(Record file, String partValue) {
    if (partValue == null) {
      return true;
    }
    Record partition = (Record) file.get(4);
    return partValue.equals(partition.get(0).toString());
  }
}
