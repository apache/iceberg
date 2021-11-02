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

package org.apache.iceberg.spark;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.functions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkTableImport extends SparkCatalogTestBase {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File fileTableDir;

  public TestSparkTableImport(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void setupTempDirs() {
    try {
      fileTableDir = temp.newFolder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        { "testhadoop", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hadoop"
            ) }
    };
  }

  @Test
  public void testImportTableForParquetPartitionedTable() throws IOException {
    String parquetTableLocation = fileTableDir.toURI().toString();

    Dataset<Row> df = spark.range(1, 2)
        .withColumn("extra_col", functions.lit(-1))
        .withColumn("struct", functions.expr("named_struct('nested_1', 'a', 'nested_2', 'd', 'nested_3', 'f')"))
        .withColumn("data", functions.lit("Z"));
    df.coalesce(1).select("id", "extra_col", "struct", "data").write()
        .format("parquet")
        .mode("append")
        .option("path", parquetTableLocation)
        .partitionBy("data")
        .saveAsTable("parquet_table");

    // don't include `extra_col` and `nested_2` on purpose
    Schema schema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            required(4, "nested_1", Types.StringType.get()),
            required(5, "nested_3", Types.StringType.get())
        )),
        required(3, "data", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("data")
        .build();
    Table table = catalog.createTable(tableIdent, schema, spec);
    File stagingDir = temp.newFolder("staging-dir");
    // Import should succeed without any validation exceptions
    SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString());
  }
}
