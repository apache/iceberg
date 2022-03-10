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
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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
  public void testDeleteFilesTable() throws Exception {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES" +
        "('format-version'='2', 'write.delete.mode'='merge-on-read')", tableName);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d")
    );
    spark.createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id=1", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<ManifestFile> expectedManifests = TestHelpers.deleteManifests(table);
    Assert.assertEquals("Should have 1 delete file", 1, expectedManifests.size());

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".delete_files").schema();

    List<Row> actual = spark.sql("SELECT * FROM " + tableName + ".delete_files").collectAsList();

    List<Record> expected = expectedEntries(table, entriesTableSchema, expectedManifests, null);

    Assert.assertEquals("Should be one delete file manifest entry", 1, expected.size());
    Assert.assertEquals("Metadata table should return one delete file", 1, actual.size());

    TestHelpers.assertEqualsSafe(filesTableSchema.asStruct(), expected.get(0), actual.get(0));
  }

  @Test
  public void testDeleteFilesTablePartitioned() throws Exception {
    sql("CREATE TABLE %s (id bigint, data string) " +
        "USING iceberg " +
        "PARTITIONED BY (data) " +
        "TBLPROPERTIES" +
        "('format-version'='2', 'write.delete.mode'='merge-on-read')", tableName);

    List<SimpleRecord> recordsA = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "a")
    );
    spark.createDataset(recordsA, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    List<SimpleRecord> recordsB = Lists.newArrayList(
        new SimpleRecord(1, "b"),
        new SimpleRecord(2, "b")
        );
    spark.createDataset(recordsB, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id=1 AND data='a'", tableName);
    sql("DELETE FROM %s WHERE id=1 AND data='b'", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<ManifestFile> expectedManifests = TestHelpers.deleteManifests(table);
    Assert.assertEquals("Should have 2 delete files", 2, expectedManifests.size());

    List<Row> actual = spark.sql("SELECT * FROM " + tableName + ".delete_files " +
        "WHERE partition.data='a'").collectAsList();

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".delete_files").schema();

    List<Record> expected = expectedEntries(table, entriesTableSchema, expectedManifests, "a");

    Assert.assertEquals("Should be one delete file manifest entry", 1, expected.size());
    Assert.assertEquals("Metadata table should return one delete file", 1, actual.size());

    TestHelpers.assertEqualsSafe(filesTableSchema.asStruct(), expected.get(0), actual.get(0));
  }

  /**
   * Find matching manifest entries of an Iceberg table
   * @param table iceberg table
   * @param entriesTableSchema schema of Manifest entries
   * @param manifestsToExplore manifests to explore of the table
   * @param partValue partition value that manifest entries must match, or null to skip filtering
   */
  private List<Record> expectedEntries(Table table, Schema entriesTableSchema,
                                       List<ManifestFile> manifestsToExplore, String partValue) throws IOException {
    List<Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : manifestsToExplore) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<Record> rows = Avro.read(in).project(
          entriesTableSchema).build()) {
        for (Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            Record file = (Record) record.get("data_file");
            if (partitionMatch(file, partValue)) {
              asDeleteRecords(file);
              expected.add(file);
            }
          }
        }
      }
    }
    return expected;
  }

  // Populate certain fields derived in the metadata tables
  private void asDeleteRecords(Record file) {
    file.put(0, FileContent.POSITION_DELETES.id());
    file.put(3, 0); // specId
  }

  private boolean partitionMatch(Record file, String partValue) {
    if (partValue == null) {
      return true;
    }
    Record partition = (Record) file.get(4);
    return partValue.equals(partition.get(0).toString());
  }
}
