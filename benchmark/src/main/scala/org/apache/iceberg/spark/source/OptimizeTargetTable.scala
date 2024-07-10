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

import org.apache.log4j.LogManager

import org.apache.spark.sql.SparkSession

object OptimizeTargetTable {

  private final val LOG = LogManager.getLogger(OptimizeTargetTable.getClass)
  private final val TARGET_TABLE_NAME = "store_sales"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("vldb-optimize-target-table")
      .getOrCreate()

    LOG.info("Successfully deployed")

    spark.sql(s"CALL iceberg.system.rewrite_data_files(table => '$TARGET_TABLE_NAME', options => map('partial-progress.enabled', 'true', 'max-concurrent-file-group-rewrites', '100'))")

    spark.sql(s"CALL iceberg.system.rewrite_manifests('$TARGET_TABLE_NAME')")
  }
}
