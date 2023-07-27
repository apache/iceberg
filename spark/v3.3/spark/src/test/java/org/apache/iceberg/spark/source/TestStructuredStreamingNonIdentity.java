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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.File;
import java.time.Instant;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestStructuredStreamingNonIdentity {

    private static final Configuration CONF = new Configuration();
    private static final Schema SCHEMA = new Schema(
            optional(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "timestamp", Types.TimestampType.withZone())
    );
    private static SparkSession spark = null;

    @Parameterized.Parameter(0)
    public boolean extensions;
    @Parameterized.Parameter(1)
    public boolean fanout;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Parameterized.Parameters(name = "extensions = {0}, fanout = {1}")
    public static Object[][] parameters() {
        return new Object[][]{
                {false, true}, // Pass
                {false, false}, // Fail because of "Skipping distribution/ordering: extensions are disabled and spec contains unsupported transforms"
                {true, true}, // Fail because of "org.apache.spark.sql.AnalysisException: days(timestamp) ASC NULLS FIRST is not currently supported"
                {true, false} // Fail because of "org.apache.spark.sql.AnalysisException: days(timestamp) ASC NULLS FIRST is not currently supported"
        };
    }

    @Before
    public void startSpark() {
        final SparkSession.Builder config = SparkSession.builder()
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", 4);

        if (extensions) {
            config.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        }

        TestStructuredStreamingNonIdentity.spark = config.getOrCreate();
    }

    @After
    public void stopSpark() {
        SparkSession currentSpark = TestStructuredStreamingNonIdentity.spark;
        TestStructuredStreamingNonIdentity.spark = null;
        currentSpark.stop();
    }

    @Test
    public void testStreamingWriteAppendMode() throws Exception {
        File parent = temp.newFolder("parquet");
        File location = new File(parent, "test-table");
        File checkpoint = new File(parent, "checkpoint");

        HadoopTables tables = new HadoopTables(CONF);
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

        Table table = tables.create(SCHEMA, spec, location.toString());
        Instant start_ts = Instant.parse("2022-01-01T01:00:00.00Z");
        List<SimpleRecordWithTimestamp> expected = Lists.newArrayList(
                new SimpleRecordWithTimestamp(1, start_ts),
                new SimpleRecordWithTimestamp(1, start_ts),
                new SimpleRecordWithTimestamp(2, start_ts)
        );

        MemoryStream<Integer> inputStream = newMemoryStream(1, spark.sqlContext(), Encoders.INT());
        DataStreamWriter<Row> streamWriter = inputStream.toDF()
                .selectExpr("value AS id", "CAST (value AS STRING) AS data", "make_timestamp(2022, 1, value, 1, 0, 0.0, 'UTC') AS timestamp")
                .writeStream()
                .outputMode("append")
                .format("iceberg")
                .option("fanout-enabled", fanout)
                .option("checkpointLocation", checkpoint.toString())
                .option("path", location.toString());

        try {
            StreamingQuery query = streamWriter.start();
            List<Integer> batch1 = Lists.newArrayList(1, 2, 1);
            send(batch1, inputStream);
            query.processAllAvailable();

            Dataset<Row> result = spark.read()
                    .format("iceberg")
                    .load(location.toString());

            List<SimpleRecordWithTimestamp> actual = result.orderBy("id").as(Encoders.bean(SimpleRecordWithTimestamp.class)).collectAsList();
            Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
            Assert.assertEquals("Result rows should match", expected, actual);
            Assert.assertEquals("Number of snapshots should match", 1, Iterables.size(table.snapshots()));
        } finally {
            for (StreamingQuery query : spark.streams().active()) {
                query.stop();
            }
        }
    }

    private <T> MemoryStream<T> newMemoryStream(int id, SQLContext sqlContext, Encoder<T> encoder) {
        return new MemoryStream<>(id, sqlContext, Option.empty(), encoder);
    }

    private <T> void send(List<T> records, MemoryStream<T> stream) {
        stream.addData(JavaConverters.asScalaBuffer(records));
    }
}
