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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestAvroWrite {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestAvroWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestAvroWrite.spark;
    TestAvroWrite.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testNestedPartitioning() throws IOException {
    Schema nestedSchema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "nestedData", Types.StructType.of(
            optional(4, "id", Types.IntegerType.get()),
            optional(5, "moreData", Types.StringType.get()))),
        optional(6, "timestamp", Types.TimestampType.withZone())
    );

    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test/iceberg");

    HadoopTables tables = new HadoopTables(new Configuration());
    PartitionSpec spec = PartitionSpec.builderFor(nestedSchema)
        .identity("id")
        .day("timestamp")
        .identity("nestedData.moreData")
        .build();
    Table table = tables.create(nestedSchema, spec, ImmutableMap.of("write.format.default", "avro"),
        location.toString());

    List<String> jsons = Lists.newArrayList(
        "{ \"id\": 1, \"data\": \"a\", \"nestedData\": { \"id\": 100, \"moreData\": \"p1\"}, " +
            "\"timestamp\": \"2017-12-01T10:12:55.034Z\" }",
        "{ \"id\": 2, \"data\": \"b\", \"nestedData\": { \"id\": 200, \"moreData\": \"p1\"}, " +
            "\"timestamp\": \"2017-12-02T10:12:55.034Z\" }",
        "{ \"id\": 3, \"data\": \"c\", \"nestedData\": { \"id\": 300, \"moreData\": \"p2\"}, " +
            "\"timestamp\": \"2017-12-03T10:12:55.034Z\" }",
        "{ \"id\": 4, \"data\": \"d\", \"nestedData\": { \"id\": 400, \"moreData\": \"p2\"}, " +
            "\"timestamp\": \"2017-12-04T10:12:55.034Z\" }"
    );
    Dataset<Row> df = spark.read().schema(SparkSchemaUtil.convert(nestedSchema))
        .json(spark.createDataset(jsons, Encoders.STRING()));

    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data", "nestedData", "timestamp").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> actual = result.orderBy("id").collectAsList();
    Assert.assertEquals("Number of rows should match", jsons.size(), actual.size());
    Assert.assertEquals("Row 1 col 1 is 1", 1, actual.get(0).getInt(0));
    Assert.assertEquals("Row 1 col 2 is a", "a", actual.get(0).getString(1));
    Assert.assertEquals("Row 1 col 3,1 is 100", 100, actual.get(0).getStruct(2).getInt(0));
    Assert.assertEquals("Row 1 col 3,2 is p1", "p1", actual.get(0).getStruct(2).getString(1));
    Assert.assertEquals("Row 1 col 4 is 2017-12-01T10:12:55.034+00:00",
        0, actual.get(0).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-01T10:12:55.034Z"))));
    Assert.assertEquals("Row 2 col 1 is 2", 2, actual.get(1).getInt(0));
    Assert.assertEquals("Row 2 col 2 is b", "b", actual.get(1).getString(1));
    Assert.assertEquals("Row 2 col 3,1 is 200", 200, actual.get(1).getStruct(2).getInt(0));
    Assert.assertEquals("Row 2 col 3,2 is p1", "p1", actual.get(1).getStruct(2).getString(1));
    Assert.assertEquals("Row 2 col 4 is 2017-12-02 12:12:55.034",
        0, actual.get(1).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-02T10:12:55.034Z"))));
    Assert.assertEquals("Row 3 col 1 is 3", 3, actual.get(2).getInt(0));
    Assert.assertEquals("Row 3 col 2 is c", "c", actual.get(2).getString(1));
    Assert.assertEquals("Row 3 col 3,1 is 300", 300, actual.get(2).getStruct(2).getInt(0));
    Assert.assertEquals("Row 3 col 3,2 is p2", "p2", actual.get(2).getStruct(2).getString(1));
    Assert.assertEquals("Row 3 col 4 is 2017-12-03 12:12:55.034",
        0, actual.get(2).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-03T10:12:55.034Z"))));
    Assert.assertEquals("Row 4 col 1 is 4", 4, actual.get(3).getInt(0));
    Assert.assertEquals("Row 4 col 2 is d", "d", actual.get(3).getString(1));
    Assert.assertEquals("Row 4 col 3,1 is 400", 400, actual.get(3).getStruct(2).getInt(0));
    Assert.assertEquals("Row 4 col 3,2 is p2", "p2", actual.get(3).getStruct(2).getString(1));
    Assert.assertEquals("Row 4 col 4 is 2017-12-04 12:12:55.034",
        0, actual.get(3).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-04T10:12:55.034Z"))));
  }
}
