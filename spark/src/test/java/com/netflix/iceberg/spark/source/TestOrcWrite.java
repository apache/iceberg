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

import com.google.common.collect.Lists;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
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

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.netflix.iceberg.types.Types.NestedField.optional;

public class TestOrcWrite {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestOrcWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestOrcWrite.spark;
    TestOrcWrite.spark = null;
    spark.stop();
  }

  @Test
  public void testBasicWrite() throws IOException {
    File parent = temp.newFolder("orc");
    File location = new File(parent, "test");
    location.mkdirs();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());
    table.updateProperties()
        .defaultFormat(FileFormat.ORC)
        .set(OrcConf.COMPRESS.getAttribute(), CompressionKind.NONE.name())
        .commit();

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(
        Encoders.bean(SimpleRecord.class)).collectAsList();

    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }
}
