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

import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/** A job that can create the target table and populate it with source data from another table. */
object PopulateTargetTable {

  private final val LOG = LogManager.getLogger(PopulateTargetTable.getClass)
  private final val TARGET_TABLE_NAME = "iceberg.store_sales"
  private final val SOURCE_BUCKET_NAME = "bucket"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("vldb-populate-target-table")
      .getOrCreate()

    LOG.info("Successfully deployed")

    spark.sql(
      s"""
        |CREATE TABLE $TARGET_TABLE_NAME (
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
        |PARTITIONED BY (bucket(256, ss_ticket_number))
        |TBLPROPERTIES (
        | 'write.object-storage.enabled' 'true'
        |)
      """.stripMargin)

    spark.conf.set(SparkSQLProperties.ADVISORY_PARTITION_SIZE, 2 * 1024 * 1024 * 1024)

    val sourceTable = s"s3a://$SOURCE_BUCKET_NAME/tpcds-iceberg/1000g_parquet_zstd_512mb_block_partitioned_iceberg/store_sales"

    val partitions = spark.read.format("iceberg")
      .load(sourceTable + "#partitions")
      .select("partition.ss_sold_date_sk")
      .collect()
      .filter(row => !row.isNullAt(0))
      .map(row => row.getInt(0))

    partitions.grouped(100).foreach { batch =>
      val predicate = batch.mkString(", ")
      LOG.info(s"Loading a batch with data matching $predicate")
      val inputDF = spark.read.format("iceberg").load(sourceTable).where(s"ss_sold_date_sk IN ($predicate)")
      inputDF.writeTo(TARGET_TABLE_NAME).append()
    }

    LOG.info(s"Loading the final batch with null values")
    val nullDF = spark.read.format("iceberg").load(sourceTable).where("ss_sold_date_sk IS NULL")
    nullDF.writeTo(TARGET_TABLE_NAME).append()
  }
}
