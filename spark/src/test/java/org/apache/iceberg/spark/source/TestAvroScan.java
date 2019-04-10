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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localOutput;

public class TestAvroScan extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestAvroScan.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestAvroScan.spark;
    TestAvroScan.spark = null;
    spark.stop();
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    File parent = temp.newFolder("avro");
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();

    File avroFile = new File(dataFolder,
        FileFormat.AVRO.addExtension(UUID.randomUUID().toString()));

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(schema, PartitionSpec.unpartitioned(), location.toString());

    // Important: use the table's schema for the rest of the test
    // When tables are created, the column ids are reassigned.
    Schema tableSchema = table.schema();

    List<Record> expected = RandomData.generateList(tableSchema, 100, 1L);

    try (FileAppender<Record> writer = Avro.write(localOutput(avroFile))
        .schema(tableSchema)
        .build()) {
      writer.addAll(expected);
    }

    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(100)
        .withFileSizeInBytes(avroFile.length())
        .withPath(avroFile.toString())
        .build();

    table.newAppend().appendFile(file).commit();

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Should contain 100 rows", 100, rows.size());

    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(tableSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
