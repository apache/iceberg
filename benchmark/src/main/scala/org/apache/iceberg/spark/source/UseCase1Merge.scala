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

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager



/**
 * A MERGE job that generates 25 updates and 25 inserts in each partition.
 */
object UseCase1Merge extends UseCaseBase {

  private final val LOG = LogManager.getLogger(UseCase1Merge.getClass)
  private final val TARGET_TABLE_STARTING_SNAPSHOT = 5074150813535168321L
  private final val MODE_SHORT = MODE.split("-").map(_.head).mkString
  private final val SOURCE_TABLE_NAME = s"store_sales_use_case_1_${MODE_SHORT}_changes"
  private final val NUM_BUCKETS = 256
  private final val NUM_UPDATES_PER_PARTITION = 25
  private final val NUM_INSERTS_PER_PARTITION = 25
  private final val NUM_OPERATIONS = 11
  private final val MERGE_RESULTS = s"use_case_1_${MODE_SHORT}_merge_results"
  private final val QUERY_RESULTS = s"use_case_1_${MODE_SHORT}_query_results"
  private final val WARMUP_RESULTS = s"use_case_1_${MODE_SHORT}_warmup_results"
  private final val MAINTENANCE_RESULTS = s"use_case_1_${MODE_SHORT}_maintenance_results"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(s"vldb-use-case-1-${MODE_SHORT}")
      .getOrCreate()

    LOG.info(s"Successfully deployed, execution mode: $MODE")

    // revert the table to the starting compacted snapshot to have predictable behavior
    spark.sql(s"CALL $CATALOG.system.rollback_to_snapshot(table => '$TARGET_TABLE_NAME', snapshot_id => $TARGET_TABLE_STARTING_SNAPSHOT)")
    spark.sql(s"CALL $CATALOG.system.expire_snapshots(table => '$TARGET_TABLE_NAME', retain_last => 10)")
    setCommonProps(spark)

    LOG.info("Create Table")
    spark.sql(s"DROP TABLE IF EXISTS $CATALOG.$SOURCE_TABLE_NAME PURGE")
    spark.sql(
      s"""
         |CREATE TABLE $CATALOG.$SOURCE_TABLE_NAME (
         | `ss_sold_date_sk` INT,
         | `ss_sold_time_sk` INT,
         | `ss_item_sk` INT NOT NULL,
         | `ss_customer_sk` INT,
         | `ss_cdemo_sk` INT,
         | `ss_hdemo_sk` INT,
         | `ss_addr_sk` INT,
         | `ss_store_sk` INT,
         | `ss_promo_sk` INT,
         | `ss_ticket_number` INT NOT NULL,
         | `ss_quantity` INT,
         | `ss_wholesale_cost` DECIMAL(7,2),
         | `ss_list_price` DECIMAL(7,2),
         | `ss_sales_price` DECIMAL(7,2),
         | `ss_ext_discount_amt` DECIMAL(7,2),
         | `ss_ext_sales_price` DECIMAL(7,2),
         | `ss_ext_wholesale_cost` DECIMAL(7,2),
         | `ss_ext_list_price` DECIMAL(7,2),
         | `ss_ext_tax` DECIMAL(7,2),
         | `ss_coupon_amt` DECIMAL(7,2),
         | `ss_net_paid` DECIMAL(7,2),
         | `ss_net_paid_inc_tax` DECIMAL(7,2),
         | `ss_net_profit` DECIMAL(7,2)
         |)
         |USING iceberg
         |PARTITIONED BY (bucket($NUM_BUCKETS, ss_ticket_number))
         |TBLPROPERTIES (
         | 'write.object-storage.enabled' 'true'
         |)
      """.stripMargin)

    val mResults = Seq.newBuilder[(Int, Long, Long, Long, Double)]
    val qResults = Seq.newBuilder[(Int, Long, Long, Long, Double)]

    LOG.info(s"Warming up")
    warmUp(spark, WARMUP_RESULTS)

    for (operation <- 0 until NUM_OPERATIONS) {
      LOG.info(s"Starting operation $operation")

      spark.sql(s"TRUNCATE TABLE $CATALOG.$SOURCE_TABLE_NAME")
      var acc = spark.table(s"$CATALOG.$SOURCE_TABLE_NAME")

      for (bucket <- 0 until NUM_BUCKETS) {
        LOG.info(s"Generating changes for bucket $bucket")

        val updates = spark.table(s"$CATALOG.$TARGET_TABLE_NAME")
          .filter(s"$CATALOG.bucket($NUM_BUCKETS, ss_ticket_number) = $bucket")
          .limit(NUM_UPDATES_PER_PARTITION)
          .selectExpr(
            "ss_sold_date_sk",
            "ss_sold_time_sk",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_cdemo_sk",
            "ss_hdemo_sk",
            "ss_addr_sk",
            "ss_store_sk",
            "ss_promo_sk",
            "ss_ticket_number",
            "-1 as ss_quantity", // update
            "ss_wholesale_cost",
            "ss_list_price",
            "ss_sales_price",
            "ss_ext_discount_amt",
            "ss_ext_sales_price",
            "ss_ext_wholesale_cost",
            "ss_ext_list_price",
            "ss_ext_tax",
            "ss_coupon_amt",
            "ss_net_paid",
            "ss_net_paid_inc_tax",
            "ss_net_profit")

        val inserts = spark.table(s"$CATALOG.$TARGET_TABLE_NAME")
          .filter(s"$CATALOG.bucket($NUM_BUCKETS, ss_ticket_number) = $bucket")
          .limit(NUM_INSERTS_PER_PARTITION)
          .selectExpr(
            "ss_sold_date_sk",
            "ss_sold_time_sk",
            s"(ss_item_sk + 500000 + $operation) as ss_item_sk", // ensure the new key is not in the table
            "ss_customer_sk",
            "ss_cdemo_sk",
            "ss_hdemo_sk",
            "ss_addr_sk",
            "ss_store_sk",
            "ss_promo_sk",
            "ss_ticket_number",
            "ss_quantity",
            "ss_wholesale_cost",
            "ss_list_price",
            "ss_sales_price",
            "ss_ext_discount_amt",
            "ss_ext_sales_price",
            "ss_ext_wholesale_cost",
            "ss_ext_list_price",
            "ss_ext_tax",
            "ss_coupon_amt",
            "ss_net_paid",
            "ss_net_paid_inc_tax",
            "ss_net_profit")

        acc = acc.union(updates)
        acc = acc.union(inserts)
      }
      acc.writeTo(s"$CATALOG.$SOURCE_TABLE_NAME").append()

      LOG.info("Configuring SQL and table properties")

      alterTable(spark)
      setCommonProps(spark)

      LOG.info("Starting the write part of the benchmark")

      val (_, mStart, mEnd, mDuration) = time {
        spark.sql(
          s"""
             |MERGE INTO $CATALOG.$TARGET_TABLE_NAME t
             |USING $CATALOG.$SOURCE_TABLE_NAME s
             |ON t.ss_ticket_number = s.ss_ticket_number AND t.ss_item_sk = s.ss_item_sk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)
      }
      mResults += ((operation, mStart, mEnd, mDuration, mDuration / 1000.0 / 1000.0 / 1000.0))

      LOG.info("Starting the read part of the benchmark")

      for (_ <- 0 until 5) {
        val (qStart, qEnd, qDuration) = runQuery(spark)
        qResults += ((operation, qStart, qEnd, qDuration, qDuration / 1000.0 / 1000.0 / 1000.0))
      }

      if (operation == 9 && MODE == RowLevelOperationMode.MERGE_ON_READ.modeName()) {
        LOG.info("Starting the maintenance part of the benchmark")

        performMinorCompaction(spark, MAINTENANCE_RESULTS)

        for (_ <- 0 until 10) {
          val (qSupportStart, qSupportEnd, qSupportDuration) = runQuery(spark)
          qResults += ((operation, qSupportStart, qSupportEnd, qSupportDuration, qSupportDuration / 1000.0 / 1000.0 / 1000.0))
        }
      }
    }

    uploadResults(spark, MERGE_RESULTS, mResults.result())
    uploadResults(spark, QUERY_RESULTS, qResults.result())
  }
}
