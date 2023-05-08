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

  // Controls whether to perform the nullability check during writes
  public static final String CHECK_NULLABILITY = "spark.sql.iceberg.check-nullability";
  public static final boolean CHECK_NULLABILITY_DEFAULT = true;

  // Controls whether to check the order of fields during writes
  public static final String CHECK_ORDERING = "spark.sql.iceberg.check-ordering";
  public static final boolean CHECK_ORDERING_DEFAULT = true;

  // Controls whether to preserve the existing grouping of data while planning splits
  public static final String PRESERVE_DATA_GROUPING =
      "spark.sql.iceberg.planning.preserve-data-grouping";
  public static final boolean PRESERVE_DATA_GROUPING_DEFAULT = false;

  // Controls whether to push down aggregate (MAX/MIN/COUNT) to Iceberg
  public static final String AGGREGATE_PUSH_DOWN_ENABLED =
      "spark.sql.iceberg.aggregate-push-down.enabled";
  public static final boolean AGGREGATE_PUSH_DOWN_ENABLED_DEFAULT = true;

  // Controls write distribution mode
  public static final String DISTRIBUTION_MODE = "spark.sql.iceberg.distribution-mode";

  // Controls the WAP ID used for write-audit-publish workflow.
  // When set, new snapshots will be staged with this ID in snapshot summary.
  public static final String WAP_ID = "spark.wap.id";

  // Controls the WAP branch used for write-audit-publish workflow.
  // When set, new snapshots will be committed to this branch.
  public static final String WAP_BRANCH = "spark.wap.branch";
}
