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
package org.apache.iceberg.spark.source

import org.apache.iceberg.RowLevelOperationMode

import org.apache.spark.sql.{DataFrame, SparkSession}

trait UseCaseBase {
  protected val MODE = RowLevelOperationMode.MERGE_ON_READ.modeName()
  protected val CATALOG = "iceberg"
  protected val TARGET_TABLE_NAME = "store_sales"
  protected val SPJ_ENABLED = true

  def time[T](f: => T): (T, Long, Long, Long) = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    (ret, start, end, end - start)
  }

  def runQuery(spark: SparkSession): (Long, Long, Long) = {
    val (_, start, end, duration) = time {
      spark.sql(
        s"""
           |SELECT
           |  COUNT(DISTINCT ss_ticket_number) AS total_transactions,
           |  SUM(ss_quantity) AS total_quantity_sold,
           |  SUM(ss_net_paid) AS total_sales,
           |  AVG(ss_quantity) AS avg_quantity_per_txn,
           |  AVG(ss_coupon_amt) AS avg_coupon_amount_used,
           |  SUM(ss_net_paid_inc_tax) AS tota_sales_including_tax,
           |  SUM(ss_net_profit) AS total_net_profit,
           |  AVG(ss_net_paid_inc_tax - ss_net_paid) as avg_tax_per_txn,
           |  AVG(ss_net_profit) AS avg_profit_per_txn,
           |  MAX(ss_net_paid) AS max_txn_value,
           |  MIN(ss_net_paid) AS min_txn_value
           |FROM $CATALOG.$TARGET_TABLE_NAME
           |""".stripMargin).collect()
    }
    (start, end, duration)
  }

  def performMinorCompaction(spark: SparkSession, table: String): Unit = {
    val supportResults = Seq.newBuilder[(Int, Long, Long, Long, Double)]
    val (_, sStart, sEnd, sDuration) = time {
      spark.sql(s"CALL iceberg.system.rewrite_data_files(table => '$TARGET_TABLE_NAME', options => map('partial-progress.enabled', 'false', 'max-concurrent-file-group-rewrites', '256', 'use-starting-sequence-number', 'false', 'min-file-size-bytes', '268435456'))")
      spark.sql(s"CALL iceberg.system.rewrite_position_delete_files(table => '$TARGET_TABLE_NAME', options => map('partial-progress.enabled', 'false', 'max-concurrent-file-group-rewrites', '256', 'rewrite-all', 'true'))")
    }
    supportResults += ((0, sStart, sEnd, sDuration, sDuration / 1000.0 / 1000.0 / 1000.0))
    uploadResults(spark, table, supportResults.result())
  }

  def warmUp(spark: SparkSession, table: String): Unit = {
    val wResults = Seq.newBuilder[(Int, Long, Long, Long, Double)]
    for (operation <- 0 until 10) {
      val (wStart, wEnd, wDuration) = runQuery(spark)
      wResults += ((operation, wStart, wEnd, wDuration, wDuration / 1000.0 / 1000.0 / 1000.0))
    }
    uploadResults(spark, table, wResults.result())
  }

  def uploadResults(spark: SparkSession, table: String, results: Seq[(Int, Long, Long, Long, Double)]): Unit = {
    import spark.implicits._
    val loc = spark.conf.get("spark.sql.catalog.iceberg.warehouse")
    results.toDF("operation", "start", "end", "duration_ns", "duration_sec").coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$loc/$table")
  }

  def setCommonProps(spark: SparkSession): Unit = {
    spark.sql("SET spark.sql.join.preferSortMergeJoin = false")
    spark.sql("SET spark.sql.shuffle.partitions = 2000")
    spark.sql("SET spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled = false")
    spark.sql(s"SET spark.sql.sources.v2.bucketing.enabled = $SPJ_ENABLED")
    spark.sql("SET `spark.sql.iceberg.planning.preserve-data-grouping` = true")
    spark.sql("SET spark.sql.sources.v2.bucketing.pushPartValues.enabled = true")
    spark.sql("SET spark.sql.requireAllClusterKeysForCoPartition = false")
    if (!SPJ_ENABLED) {
      spark.sql("SET `spark.sql.iceberg.advisory-partition-size` = 536870912")
    }
  }

  def alterTable(spark: SparkSession): Unit = {
    spark.sql(s"ALTER TABLE $CATALOG.$TARGET_TABLE_NAME SET TBLPROPERTIES ('write.merge.mode' '$MODE', 'write.merge.distribution-mode' '${if (SPJ_ENABLED) "none" else "hash"}')")
  }
}
