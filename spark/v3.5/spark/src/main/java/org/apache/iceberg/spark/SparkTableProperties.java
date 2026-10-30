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

/** Spark-specific table properties that control Spark integration behavior. */
public class SparkTableProperties {

  private SparkTableProperties() {}

  public static final String WRITE_PARTITIONED_FANOUT_ENABLED = "write.spark.fanout.enabled";
  public static final boolean WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT = false;

  public static final String WRITE_ACCEPT_ANY_SCHEMA = "write.spark.accept-any-schema";
  public static final boolean WRITE_ACCEPT_ANY_SCHEMA_DEFAULT = false;

  public static final String WRITE_AUTO_SCHEMA_EVOLUTION =
      "write.spark.auto-schema-evolution.enabled";
  public static final boolean WRITE_AUTO_SCHEMA_EVOLUTION_DEFAULT = true;

  public static final String WRITE_ADVISORY_PARTITION_SIZE_BYTES =
      "write.spark.advisory-partition-size-bytes";
}
