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

package com.netflix.iceberg.spark.source;

import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.orc.ORC;
import com.netflix.iceberg.orc.OrcFileAppender;
import com.netflix.iceberg.spark.data.AvroDataTest;
import com.netflix.iceberg.spark.data.RandomData;
import com.netflix.iceberg.spark.data.SparkOrcWriter;
import com.netflix.iceberg.spark.data.TestHelpers;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.storage.serde2.io.DateWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.netflix.iceberg.Files.localOutput;

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
    try (SparkOrcWriter writer =
             new SparkOrcWriter(ORC.write(localOutput(orcFile))
                                   .schema(tableSchema)
                                   .build())) {
      writer.addAll(RandomData.generateSpark(tableSchema, ROW_COUNT, SEED));
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
    for(int i=0; i < ROW_COUNT; ++i) {
      TestHelpers.assertEquals("row " + i, schema.asStruct(), expected.next(),
          rows.get(i));
    }
  }
}
