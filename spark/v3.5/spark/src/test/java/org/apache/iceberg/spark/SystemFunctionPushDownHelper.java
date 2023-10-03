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

import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.SparkSession;

public class SystemFunctionPushDownHelper {
  public static final Types.StructType STRUCT =
      Types.StructType.of(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private SystemFunctionPushDownHelper() {}

  public static void createUnpartitionedTable(SparkSession spark, String tableName) {
    sql(spark, "CREATE TABLE %s (id BIGINT, ts TIMESTAMP, data STRING) USING iceberg", tableName);
    insertRecords(spark, tableName);
  }

  public static void createPartitionedTable(
      SparkSession spark, String tableName, String partitionCol) {
    sql(
        spark,
        "CREATE TABLE %s (id BIGINT, ts TIMESTAMP, data STRING) USING iceberg PARTITIONED BY (%s)",
        tableName,
        partitionCol);
    insertRecords(spark, tableName);
  }

  private static void insertRecords(SparkSession spark, String tableName) {
    sql(
        spark,
        "ALTER TABLE %s SET TBLPROPERTIES('%s' %s)",
        tableName,
        "read.split.target-size",
        "10");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(0, CAST('2017-11-22T09:20:44.294658+00:00' AS TIMESTAMP), 'data-0')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(1, CAST('2017-11-22T07:15:34.582910+00:00' AS TIMESTAMP), 'data-1')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(2, CAST('2017-11-22T06:02:09.243857+00:00' AS TIMESTAMP), 'data-2')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(3, CAST('2017-11-22T03:10:11.134509+00:00' AS TIMESTAMP), 'data-3')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(4, CAST('2017-11-22T00:34:00.184671+00:00' AS TIMESTAMP), 'data-4')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(5, CAST('2018-12-21T22:20:08.935889+00:00' AS TIMESTAMP), 'material-5')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(6, CAST('2018-12-21T21:55:30.589712+00:00' AS TIMESTAMP), 'material-6')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(7, CAST('2018-12-21T17:31:14.532797+00:00' AS TIMESTAMP), 'material-7')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(8, CAST('2018-12-21T15:21:51.237521+00:00' AS TIMESTAMP), 'material-8')");
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tableName,
        "(9, CAST('2018-12-21T15:02:15.230570+00:00' AS TIMESTAMP), 'material-9')");
  }

  public static int timestampStrToYearOrdinal(String timestamp) {
    return DateTimeUtil.microsToYears(DateTimeUtil.isoTimestamptzToMicros(timestamp));
  }

  public static int timestampStrToMonthOrdinal(String timestamp) {
    return DateTimeUtil.microsToMonths(DateTimeUtil.isoTimestamptzToMicros(timestamp));
  }

  public static int timestampStrToDayOrdinal(String timestamp) {
    return DateTimeUtil.microsToDays(DateTimeUtil.isoTimestamptzToMicros(timestamp));
  }

  public static int timestampStrToHourOrdinal(String timestamp) {
    return DateTimeUtil.microsToHours(DateTimeUtil.isoTimestamptzToMicros(timestamp));
  }

  private static void sql(SparkSession spark, String sqlFormat, Object... args) {
    spark.sql(String.format(sqlFormat, args));
  }
}
