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

public class SparkSQLProperties {

  private SparkSQLProperties() {}

  // Controls whether vectorized reads are enabled
  public static final String VECTORIZATION_ENABLED = "spark.sql.iceberg.vectorization.enabled";

  // Controls whether reading/writing timestamps without timezones is allowed
  public static final String HANDLE_TIMESTAMP_WITHOUT_TIMEZONE =
      "spark.sql.iceberg.handle-timestamp-without-timezone";
  public static final boolean HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT = false;

  // Controls whether timestamp types for new tables should be stored with timezone info
  public static final String USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES =
      "spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables";
  public static final boolean USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES_DEFAULT = false;

  // Controls whether to perform the nullability check during writes
  public static final String CHECK_NULLABILITY = "spark.sql.iceberg.check-nullability";
  public static final boolean CHECK_NULLABILITY_DEFAULT = true;

  // Controls whether to check the order of fields during writes
  public static final String CHECK_ORDERING = "spark.sql.iceberg.check-ordering";
  public static final boolean CHECK_ORDERING_DEFAULT = true;
}
