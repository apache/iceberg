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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDataFrameWriterV2Coercion extends TestBaseWithCatalog {

  @Parameters(
      name = "catalogName = {0}, implementation = {1}, config = {2}, format = {3}, dataType = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      parameter(FileFormat.AVRO, "byte"),
      parameter(FileFormat.ORC, "byte"),
      parameter(FileFormat.PARQUET, "byte"),
      parameter(FileFormat.AVRO, "short"),
      parameter(FileFormat.ORC, "short"),
      parameter(FileFormat.PARQUET, "short")
    };
  }

  private static Object[] parameter(FileFormat fileFormat, String dataType) {
    return new Object[] {
      SparkCatalogConfig.HADOOP.catalogName(),
      SparkCatalogConfig.HADOOP.implementation(),
      SparkCatalogConfig.HADOOP.properties(),
      fileFormat,
      dataType
    };
  }

  @Parameter(index = 3)
  private FileFormat format;

  @Parameter(index = 4)
  private String dataType;

  @TestTemplate
  public void testByteAndShortCoercion() {

    Dataset<Row> df =
        jsonToDF(
            "id " + dataType + ", data string",
            "{ \"id\": 1, \"data\": \"a\" }",
            "{ \"id\": 2, \"data\": \"b\" }");

    df.writeTo(tableName).option("write-format", format.name()).createOrReplace();

    assertEquals(
        "Should have initial 2-column rows",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("select * from %s order by id", tableName));
  }
}
