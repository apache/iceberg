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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDataFrameWriterV2Coercion extends SparkTestBaseWithCatalog {

  private final FileFormat format;
  private final String dataType;

  public TestDataFrameWriterV2Coercion(FileFormat format, String dataType) {
    this.format = format;
    this.dataType = dataType;
  }

  @Parameterized.Parameters(name = "format = {0}, dataType = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.AVRO, "byte"},
      new Object[] {FileFormat.ORC, "byte"},
      new Object[] {FileFormat.PARQUET, "byte"},
      new Object[] {FileFormat.AVRO, "short"},
      new Object[] {FileFormat.ORC, "short"},
      new Object[] {FileFormat.PARQUET, "short"}
    };
  }

  @Test
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
