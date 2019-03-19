/*
 * Copyright 2018 Hortonworks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.source;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.iceberg.Files.localOutput;

public class TestOrcScan extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestOrcScan.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestOrcScan.spark;
    TestOrcScan.spark = null;
    spark.stop();
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    System.out.println("Starting ORC test with " + schema);
    final int ROW_COUNT = 100;
    final long SEED = 1;
    File parent = temp.newFolder("orc");
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();

    File orcFile = new File(dataFolder,
        FileFormat.ORC.addExtension(UUID.randomUUID().toString()));

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(schema, PartitionSpec.unpartitioned(),
        location.toString());

    // Important: use the table's schema for the rest of the test
    // When tables are created, the column ids are reassigned.
    Schema tableSchema = table.schema();

    Metrics metrics;
    SparkOrcWriter writer = new SparkOrcWriter(ORC.write(localOutput(orcFile))
        .schema(tableSchema)
        .build());
    try {
      writer.addAll(RandomData.generateSpark(tableSchema, ROW_COUNT, SEED));
    } finally {
      writer.close();
      // close writes the last batch, so metrics are not correct until after close is called
      metrics = writer.metrics();
    }

    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withFileSizeInBytes(orcFile.length())
        .withPath(orcFile.toString())
        .withMetrics(metrics)
        .build();

    table.newAppend().appendFile(file).commit();

    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Wrong number of rows", ROW_COUNT, rows.size());
    Iterator<InternalRow> expected = RandomData.generateSpark(tableSchema,
        ROW_COUNT, SEED);
    for(int i = 0; i < ROW_COUNT; ++i) {
      final InternalRow expectedRow = expected.next();  // useful for debug
      TestHelpers.assertEquals("row " + i, schema.asStruct(), expectedRow,
          rows.get(i));
    }
  }
}
