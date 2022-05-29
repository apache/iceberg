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
  public void testPartitionedTable() throws Exception {
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

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();

    List<ManifestFile> expectedDataManifests = table.currentSnapshot().dataManifests();
    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());

    Schema filesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".files").schema();

    // Check data files table
    List<Record> expectedDataFiles = expectedEntries(table, FileContent.DATA,
        entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should have one data file manifest entry", 1, expectedDataFiles.size());

    List<Row> actualDataFiles = spark.sql("SELECT * FROM " + tableName + ".files " +
        "WHERE partition.data='a'").collectAsList();
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(filesTableSchema.asStruct(), expectedDataFiles.get(0), actualDataFiles.get(0));

    // Check partitions table
    List<Row> actualPartitionsWithProjection =
        spark.sql("SELECT file_count FROM " + tableName + ".partitions ").collectAsList();
    Assert.assertEquals("Metadata table should return two partitions record", 2,
        actualPartitionsWithProjection.size());
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(1, actualPartitionsWithProjection.get(i).get(0));
    }
  }


  /**
   * Find matching manifest entries of an Iceberg table
   * @param table iceberg table
   * @param expectedContent file content to populate on entries
   * @param entriesTableSchema schema of Manifest entries
   * @param manifestsToExplore manifests to explore of the table
   * @param partValue partition value that manifest entries must match, or null to skip filtering
   */
  private List<Record> expectedEntries(Table table, FileContent expectedContent, Schema entriesTableSchema,
                                       List<ManifestFile> manifestsToExplore, String partValue) throws IOException {
    List<Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : manifestsToExplore) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<Record> rows = Avro.read(in).project(entriesTableSchema).build()) {
        for (Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            Record file = (Record) record.get("data_file");
            if (partitionMatch(file, partValue)) {
              asMetadataRecord(file, expectedContent);
              expected.add(file);
            }
          }
        }
      }
    }
    return expected;
  }

  // Populate certain fields derived in the metadata tables
  private void asMetadataRecord(Record file, FileContent content) {
    file.put(0, content.id());
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
