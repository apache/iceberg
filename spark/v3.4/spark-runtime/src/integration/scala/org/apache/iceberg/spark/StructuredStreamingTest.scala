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

package org.apache.iceberg.spark

import java.nio.file.Files
import java.sql.Timestamp
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.junit.Test

case class Bla(ts: Timestamp, a: String, b: Double)

class StructuredStreamingTest {

  @Test
  def testStructuredStreaming(): Unit = {
    val warehouseDir = Files.createTempDirectory("spark-warehouse-iceberg-").toString
    val checkpointDir = Files.createTempDirectory("spark-checkpoint-").toString
    val spark = SparkSession.builder()
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehouseDir)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .appName("BugRepro")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      "create table test_iceberg_table(ts timestamp, a string, b double) " +
      "using iceberg partitioned by (days(ts))"
    )

    implicit val sqlContext = spark.sqlContext
    implicit val encoder = Encoders.product[Bla]
    val memStream = MemoryStream[Bla]
    val now = System.currentTimeMillis()
    val day = 86400000
    memStream.addData(List(
      Bla(new Timestamp(now), "test", 12.34),
      Bla(new Timestamp(now - 1 * day), "test 1d", 33.34),
      Bla(new Timestamp(now - 3 * day), "test 3d", 44.34),
      Bla(new Timestamp(now - 2 * day), "test 2d", 55.34),
    ))

    memStream.toDF()
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .option("path", "test_iceberg_table")
      .option("fanout-enabled", true)
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.AvailableNow())
      .start()
      .awaitTermination()
  }
}
