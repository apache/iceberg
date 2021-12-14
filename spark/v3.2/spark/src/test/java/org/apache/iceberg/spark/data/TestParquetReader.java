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

package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetReader {
  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    TestParquetReader.spark = SparkSession.builder().master("local[2]")
            .config("spark.sql.parquet.writeLegacyFormat", true)
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestParquetReader.spark;
    TestParquetReader.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testHiveStyleThreeLevelList() throws IOException {
    File location = new File(temp.getRoot(), "parquetReaderTest");
    StructType sparkSchema =
            new StructType(
                    new StructField[]{
                            new StructField(
                                    "col1", new ArrayType(
                                    new StructType(
                                            new StructField[]{
                                                    new StructField(
                                                            "col2",
                                                            DataTypes.IntegerType,
                                                            false,
                                                            Metadata.empty())
                                            }), true), true, Metadata.empty())});

    String expectedParquetSchema =
            "message spark_schema {\n" +
                    "  optional group col1 (LIST) {\n" +
                    "    repeated group bag {\n" +
                    "      optional group array {\n" +
                    "        required int32 col2;\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}\n";


    // generate parquet file with required schema
    List<String> testData = Collections.singletonList("{\"col1\": [{\"col2\": 1}]}");
    spark.read().schema(sparkSchema).json(
                    JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(testData))
            .coalesce(1).write().format("parquet").mode(SaveMode.Append).save(location.getPath());

    File parquetFile = Arrays.stream(Objects.requireNonNull(location.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("parquet");
      }
    }))).findAny().get();

    // verify generated parquet file has expected schema
    ParquetFileReader pqReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFile.getPath()), new Configuration()));
    MessageType schema = pqReader.getFooter().getFileMetaData().getSchema();
    Assert.assertEquals(expectedParquetSchema, schema.toString());

    // read from Iceberg's parquet reader and ensure data is read correctly into Spark's internal row
    Schema icebergSchema = SparkSchemaUtil.convert(sparkSchema);
    List<InternalRow> rows;
    try (CloseableIterable<InternalRow> reader =
                 Parquet.read(Files.localInput(parquetFile.getPath()))
                         .project(icebergSchema)
                         .withNameMapping(MappingUtil.create(icebergSchema))
                         .createReaderFunc(type -> SparkParquetReaders.buildReader(icebergSchema, type))
                         .build()) {
      rows = Lists.newArrayList(reader);
    }
    Assert.assertEquals(1, rows.size());
    InternalRow row = rows.get(0);
    Assert.assertEquals(1, row.getArray(0).getStruct(0, 1).getInt(0));
  }
}
