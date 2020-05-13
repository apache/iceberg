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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIdentityPartitionData  {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" },
        new Object[] { "orc" }
    };
  }

  private final String format;

  public TestIdentityPartitionData(String format) {
    this.format = format;
  }

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestIdentityPartitionData.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestIdentityPartitionData.spark;
    TestIdentityPartitionData.spark = null;
    currentSpark.stop();
  }

  private static final Schema LOG_SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "date", Types.StringType.get()),
      Types.NestedField.optional(3, "level", Types.StringType.get()),
      Types.NestedField.optional(4, "message", Types.StringType.get())
  );

  private static final List<LogMessage> LOGS = ImmutableList.of(
      LogMessage.debug("2020-02-02", "debug event 1"),
      LogMessage.info("2020-02-02", "info event 1"),
      LogMessage.debug("2020-02-02", "debug event 2"),
      LogMessage.info("2020-02-03", "info event 2"),
      LogMessage.debug("2020-02-03", "debug event 3"),
      LogMessage.info("2020-02-03", "info event 3"),
      LogMessage.error("2020-02-03", "error event 1"),
      LogMessage.debug("2020-02-04", "debug event 4"),
      LogMessage.warn("2020-02-04", "warn event 1"),
      LogMessage.debug("2020-02-04", "debug event 5")
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private PartitionSpec spec = PartitionSpec.builderFor(LOG_SCHEMA).identity("date").identity("level").build();
  private Table table = null;
  private Dataset<Row> logs = null;

  @Before
  public void setupTable() throws Exception {
    File location = temp.newFolder("logs");
    Assert.assertTrue("Temp folder should exist", location.exists());

    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format);
    this.table = TABLES.create(LOG_SCHEMA, spec, properties, location.toString());
    this.logs = spark.createDataFrame(LOGS, LogMessage.class).select("id", "date", "level", "message");

    logs.orderBy("date", "level", "id").write().format("iceberg").mode("append").save(location.toString());
  }

  @Test
  public void testFullProjection() {
    List<Row> expected = logs.orderBy("id").collectAsList();
    List<Row> actual = spark.read().format("iceberg").load(table.location()).orderBy("id").collectAsList();
    Assert.assertEquals("Rows should match", expected, actual);
  }

  @Test
  public void testProjections() {
    String[][] cases = new String[][] {
        // individual fields
        new String[] { "date" },
        new String[] { "level" },
        new String[] { "message" },
        // field pairs
        new String[] { "date", "message" },
        new String[] { "level", "message" },
        new String[] { "date", "level" },
        // out-of-order pairs
        new String[] { "message", "date" },
        new String[] { "message", "level" },
        new String[] { "level", "date" },
        // full projection, different orderings
        new String[] { "date", "level", "message" },
        new String[] { "level", "date", "message" },
        new String[] { "date", "message", "level" },
        new String[] { "level", "message", "date" },
        new String[] { "message", "date", "level" },
        new String[] { "message", "level", "date" }
    };

    for (String[] ordering : cases) {
      List<Row> expected = logs.select("id", ordering).orderBy("id").collectAsList();
      List<Row> actual = spark.read()
          .format("iceberg").load(table.location())
          .select("id", ordering).orderBy("id")
          .collectAsList();
      Assert.assertEquals("Rows should match for ordering: " + Arrays.toString(ordering), expected, actual);
    }
  }
}

