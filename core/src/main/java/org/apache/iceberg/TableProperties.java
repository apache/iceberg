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
package org.apache.iceberg;

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class TableProperties {

  private TableProperties() {}

  /**
   * Reserved table property for table format version.
   *
   * <p>Iceberg will default a new table's format version to the latest stable and recommended
   * version. This reserved property keyword allows users to override the Iceberg format version of
   * the table metadata.
   *
   * <p>If this table property exists when creating a table, the table will use the specified format
   * version. If a table updates this property, it will try to upgrade to the specified format
   * version.
   *
   * <p>Note: incomplete or unstable versions cannot be selected using this property.
   */
  public static final String FORMAT_VERSION = "format-version";

  /** Reserved table property for table UUID. */
  public static final String UUID = "uuid";

  /** Reserved table property for the total number of snapshots. */
  public static final String SNAPSHOT_COUNT = "snapshot-count";

  /** Reserved table property for current snapshot summary. */
  public static final String CURRENT_SNAPSHOT_SUMMARY = "current-snapshot-summary";

  /** Reserved table property for current snapshot id. */
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";

  /** Reserved table property for current snapshot timestamp. */
  public static final String CURRENT_SNAPSHOT_TIMESTAMP = "current-snapshot-timestamp-ms";

  /** Reserved table property for the JSON representation of current schema. */
  public static final String CURRENT_SCHEMA = "current-schema";

  /** Reserved table property for the JSON representation of current(default) partition spec. */
  public static final String DEFAULT_PARTITION_SPEC = "default-partition-spec";

  /** Reserved table property for the JSON representation of current(default) sort order. */
  public static final String DEFAULT_SORT_ORDER = "default-sort-order";

  /**
   * Reserved Iceberg table properties list.
   *
   * <p>Reserved table properties are only used to control behaviors when creating or updating a
   * table. The value of these properties are not persisted as a part of the table metadata.
   */
  public static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(
          FORMAT_VERSION,
          UUID,
          SNAPSHOT_COUNT,
          CURRENT_SNAPSHOT_ID,
          CURRENT_SNAPSHOT_SUMMARY,
          CURRENT_SNAPSHOT_TIMESTAMP,
          CURRENT_SCHEMA,
          DEFAULT_PARTITION_SPEC,
          DEFAULT_SORT_ORDER);

  public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
  public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

  public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
  public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;

  public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
  public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60 * 1000; // 1 minute

  public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
  public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 30 * 60 * 1000; // 30 minutes

  public static final String COMMIT_NUM_STATUS_CHECKS = "commit.status-check.num-retries";
  public static final int COMMIT_NUM_STATUS_CHECKS_DEFAULT = 3;

  public static final String COMMIT_STATUS_CHECKS_MIN_WAIT_MS = "commit.status-check.min-wait-ms";
  public static final long COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT = 1000; // 1 second

  public static final String COMMIT_STATUS_CHECKS_MAX_WAIT_MS = "commit.status-check.max-wait-ms";
  public static final long COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT = 60 * 1000; // 1 minute

  public static final String COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS =
      "commit.status-check.total-timeout-ms";
  public static final long COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT =
      30 * 60 * 1000; // 30 minutes

  public static final String MANIFEST_TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes";
  public static final long MANIFEST_TARGET_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024; // 8 MB

  public static final String MANIFEST_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge";
  public static final int MANIFEST_MIN_MERGE_COUNT_DEFAULT = 100;

  public static final String MANIFEST_MERGE_ENABLED = "commit.manifest-merge.enabled";
  public static final boolean MANIFEST_MERGE_ENABLED_DEFAULT = true;

  public static final String DEFAULT_FILE_FORMAT = "write.format.default";
  public static final String DELETE_DEFAULT_FILE_FORMAT = "write.delete.format.default";
  public static final String DEFAULT_FILE_FORMAT_DEFAULT = "parquet";

  public static final String PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes";
  public static final String DELETE_PARQUET_ROW_GROUP_SIZE_BYTES =
      "write.delete.parquet.row-group-size-bytes";
  public static final int PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT = 128 * 1024 * 1024; // 128 MB

  public static final String PARQUET_PAGE_SIZE_BYTES = "write.parquet.page-size-bytes";
  public static final String DELETE_PARQUET_PAGE_SIZE_BYTES =
      "write.delete.parquet.page-size-bytes";
  public static final int PARQUET_PAGE_SIZE_BYTES_DEFAULT = 1024 * 1024; // 1 MB

  public static final String PARQUET_PAGE_ROW_LIMIT = "write.parquet.page-row-limit";
  public static final String DELETE_PARQUET_PAGE_ROW_LIMIT = "write.delete.parquet.page-row-limit";
  public static final int PARQUET_PAGE_ROW_LIMIT_DEFAULT = 20_000;

  public static final String PARQUET_DICT_SIZE_BYTES = "write.parquet.dict-size-bytes";
  public static final String DELETE_PARQUET_DICT_SIZE_BYTES =
      "write.delete.parquet.dict-size-bytes";
  public static final int PARQUET_DICT_SIZE_BYTES_DEFAULT = 2 * 1024 * 1024; // 2 MB

  public static final String PARQUET_COMPRESSION = "write.parquet.compression-codec";
  public static final String DELETE_PARQUET_COMPRESSION = "write.delete.parquet.compression-codec";
  public static final String PARQUET_COMPRESSION_DEFAULT = "gzip";
  public static final String PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0 = "zstd";

  public static final String PARQUET_COMPRESSION_LEVEL = "write.parquet.compression-level";
  public static final String DELETE_PARQUET_COMPRESSION_LEVEL =
      "write.delete.parquet.compression-level";
  public static final String PARQUET_COMPRESSION_LEVEL_DEFAULT = null;

  public static final String PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT =
      "write.parquet.row-group-check-min-record-count";
  public static final String DELETE_PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT =
      "write.delete.parquet.row-group-check-min-record-count";
  public static final int PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT = 100;

  public static final String PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT =
      "write.parquet.row-group-check-max-record-count";
  public static final String DELETE_PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT =
      "write.delete.parquet.row-group-check-max-record-count";
  public static final int PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT = 10000;

  public static final String PARQUET_BLOOM_FILTER_MAX_BYTES =
      "write.parquet.bloom-filter-max-bytes";
  public static final int PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT = 1024 * 1024;

  public static final String PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX =
      "write.parquet.bloom-filter-enabled.column.";

  public static final String AVRO_COMPRESSION = "write.avro.compression-codec";
  public static final String DELETE_AVRO_COMPRESSION = "write.delete.avro.compression-codec";
  public static final String AVRO_COMPRESSION_DEFAULT = "gzip";

  public static final String AVRO_COMPRESSION_LEVEL = "write.avro.compression-level";
  public static final String DELETE_AVRO_COMPRESSION_LEVEL = "write.delete.avro.compression-level";
  public static final String AVRO_COMPRESSION_LEVEL_DEFAULT = null;

  public static final String ORC_STRIPE_SIZE_BYTES = "write.orc.stripe-size-bytes";

  public static final String ORC_BLOOM_FILTER_COLUMNS = "write.orc.bloom.filter.columns";
  public static final String ORC_BLOOM_FILTER_COLUMNS_DEFAULT = "";

  public static final String ORC_BLOOM_FILTER_FPP = "write.orc.bloom.filter.fpp";
  public static final double ORC_BLOOM_FILTER_FPP_DEFAULT = 0.05;

  public static final String DELETE_ORC_STRIPE_SIZE_BYTES = "write.delete.orc.stripe-size-bytes";
  public static final long ORC_STRIPE_SIZE_BYTES_DEFAULT = 64L * 1024 * 1024; // 64 MB

  public static final String ORC_BLOCK_SIZE_BYTES = "write.orc.block-size-bytes";
  public static final String DELETE_ORC_BLOCK_SIZE_BYTES = "write.delete.orc.block-size-bytes";
  public static final long ORC_BLOCK_SIZE_BYTES_DEFAULT = 256L * 1024 * 1024; // 256 MB

  public static final String ORC_WRITE_BATCH_SIZE = "write.orc.vectorized.batch-size";
  public static final String DELETE_ORC_WRITE_BATCH_SIZE = "write.delete.orc.vectorized.batch-size";
  public static final int ORC_WRITE_BATCH_SIZE_DEFAULT = 1024;

  public static final String ORC_COMPRESSION = "write.orc.compression-codec";
  public static final String DELETE_ORC_COMPRESSION = "write.delete.orc.compression-codec";
  public static final String ORC_COMPRESSION_DEFAULT = "zlib";

  public static final String ORC_COMPRESSION_STRATEGY = "write.orc.compression-strategy";
  public static final String DELETE_ORC_COMPRESSION_STRATEGY =
      "write.delete.orc.compression-strategy";
  public static final String ORC_COMPRESSION_STRATEGY_DEFAULT = "speed";

  public static final String SPLIT_SIZE = "read.split.target-size";
  public static final long SPLIT_SIZE_DEFAULT = 128 * 1024 * 1024; // 128 MB

  public static final String METADATA_SPLIT_SIZE = "read.split.metadata-target-size";
  public static final long METADATA_SPLIT_SIZE_DEFAULT = 32 * 1024 * 1024; // 32 MB

  public static final String SPLIT_LOOKBACK = "read.split.planning-lookback";
  public static final int SPLIT_LOOKBACK_DEFAULT = 10;

  public static final String SPLIT_OPEN_FILE_COST = "read.split.open-file-cost";
  public static final long SPLIT_OPEN_FILE_COST_DEFAULT = 4 * 1024 * 1024; // 4MB

  public static final String ADAPTIVE_SPLIT_SIZE_ENABLED = "read.split.adaptive-size.enabled";
  public static final boolean ADAPTIVE_SPLIT_SIZE_ENABLED_DEFAULT = true;

  public static final String PARQUET_VECTORIZATION_ENABLED = "read.parquet.vectorization.enabled";
  public static final boolean PARQUET_VECTORIZATION_ENABLED_DEFAULT = true;

  public static final String PARQUET_BATCH_SIZE = "read.parquet.vectorization.batch-size";
  public static final int PARQUET_BATCH_SIZE_DEFAULT = 5000;

  public static final String ORC_VECTORIZATION_ENABLED = "read.orc.vectorization.enabled";
  public static final boolean ORC_VECTORIZATION_ENABLED_DEFAULT = false;

  public static final String ORC_BATCH_SIZE = "read.orc.vectorization.batch-size";
  public static final int ORC_BATCH_SIZE_DEFAULT = 5000;

  public static final String DATA_PLANNING_MODE = "read.data-planning-mode";
  public static final String DELETE_PLANNING_MODE = "read.delete-planning-mode";
  public static final String PLANNING_MODE_DEFAULT = PlanningMode.AUTO.modeName();

  public static final String OBJECT_STORE_ENABLED = "write.object-storage.enabled";
  public static final boolean OBJECT_STORE_ENABLED_DEFAULT = false;

  /** @deprecated Use {@link #WRITE_DATA_LOCATION} instead. */
  @Deprecated public static final String OBJECT_STORE_PATH = "write.object-storage.path";

  public static final String WRITE_LOCATION_PROVIDER_IMPL = "write.location-provider.impl";

  /** @deprecated Use {@link #WRITE_DATA_LOCATION} instead. */
  @Deprecated
  public static final String WRITE_FOLDER_STORAGE_LOCATION = "write.folder-storage.path";

  // This only applies to files written after this property is set. Files previously written aren't
  // relocated to reflect this parameter.
  // If not set, defaults to a "data" folder underneath the root path of the table.
  public static final String WRITE_DATA_LOCATION = "write.data.path";

  // This only applies to files written after this property is set. Files previously written aren't
  // relocated to reflect this parameter.
  // If not set, defaults to a "metadata" folder underneath the root path of the table.
  public static final String WRITE_METADATA_LOCATION = "write.metadata.path";

  public static final String WRITE_PARTITION_SUMMARY_LIMIT = "write.summary.partition-limit";
  public static final int WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT = 0;

  /** @deprecated will be removed in 2.0.0, writing manifest lists is always enabled */
  @Deprecated public static final String MANIFEST_LISTS_ENABLED = "write.manifest-lists.enabled";

  /** @deprecated will be removed in 2.0.0, writing manifest lists is always enabled */
  @Deprecated public static final boolean MANIFEST_LISTS_ENABLED_DEFAULT = true;

  public static final String METADATA_COMPRESSION = "write.metadata.compression-codec";
  public static final String METADATA_COMPRESSION_DEFAULT = "none";

  public static final String METADATA_PREVIOUS_VERSIONS_MAX =
      "write.metadata.previous-versions-max";
  public static final int METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT = 100;

  // This enables to delete the oldest metadata file after commit.
  public static final String METADATA_DELETE_AFTER_COMMIT_ENABLED =
      "write.metadata.delete-after-commit.enabled";
  public static final boolean METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT = false;

  public static final String METRICS_MAX_INFERRED_COLUMN_DEFAULTS =
      "write.metadata.metrics.max-inferred-column-defaults";
  public static final int METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT = 100;

  public static final String METRICS_MODE_COLUMN_CONF_PREFIX = "write.metadata.metrics.column.";
  public static final String DEFAULT_WRITE_METRICS_MODE = "write.metadata.metrics.default";
  public static final String DEFAULT_WRITE_METRICS_MODE_DEFAULT = "truncate(16)";

  public static final String DEFAULT_NAME_MAPPING = "schema.name-mapping.default";

  public static final String WRITE_AUDIT_PUBLISH_ENABLED = "write.wap.enabled";
  public static final String WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT = "false";

  public static final String WRITE_TARGET_FILE_SIZE_BYTES = "write.target-file-size-bytes";
  public static final long WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT = 512 * 1024 * 1024; // 512 MB

  public static final String DELETE_TARGET_FILE_SIZE_BYTES = "write.delete.target-file-size-bytes";
  public static final long DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT = 64 * 1024 * 1024; // 64 MB

  public static final String SPARK_WRITE_PARTITIONED_FANOUT_ENABLED = "write.spark.fanout.enabled";
  public static final boolean SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT = false;

  public static final String SPARK_WRITE_ACCEPT_ANY_SCHEMA = "write.spark.accept-any-schema";
  public static final boolean SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT = false;

  public static final String SPARK_WRITE_ADVISORY_PARTITION_SIZE_BYTES =
      "write.spark.advisory-partition-size-bytes";

  public static final String SNAPSHOT_ID_INHERITANCE_ENABLED =
      "compatibility.snapshot-id-inheritance.enabled";
  public static final boolean SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT = false;

  public static final String ENGINE_HIVE_ENABLED = "engine.hive.enabled";
  public static final boolean ENGINE_HIVE_ENABLED_DEFAULT = false;

  public static final String HIVE_LOCK_ENABLED = "engine.hive.lock-enabled";
  public static final boolean HIVE_LOCK_ENABLED_DEFAULT = true;

  public static final String WRITE_DISTRIBUTION_MODE = "write.distribution-mode";
  public static final String WRITE_DISTRIBUTION_MODE_NONE = "none";
  public static final String WRITE_DISTRIBUTION_MODE_HASH = "hash";
  public static final String WRITE_DISTRIBUTION_MODE_RANGE = "range";

  public static final String GC_ENABLED = "gc.enabled";
  public static final boolean GC_ENABLED_DEFAULT = true;

  public static final String MAX_SNAPSHOT_AGE_MS = "history.expire.max-snapshot-age-ms";
  public static final long MAX_SNAPSHOT_AGE_MS_DEFAULT = 5 * 24 * 60 * 60 * 1000; // 5 days

  public static final String MIN_SNAPSHOTS_TO_KEEP = "history.expire.min-snapshots-to-keep";
  public static final int MIN_SNAPSHOTS_TO_KEEP_DEFAULT = 1;

  public static final String MAX_REF_AGE_MS = "history.expire.max-ref-age-ms";
  public static final long MAX_REF_AGE_MS_DEFAULT = Long.MAX_VALUE;

  public static final String DELETE_ISOLATION_LEVEL = "write.delete.isolation-level";
  public static final String DELETE_ISOLATION_LEVEL_DEFAULT = "serializable";

  public static final String DELETE_MODE = "write.delete.mode";
  public static final String DELETE_MODE_DEFAULT = RowLevelOperationMode.COPY_ON_WRITE.modeName();

  public static final String DELETE_DISTRIBUTION_MODE = "write.delete.distribution-mode";

  public static final String UPDATE_ISOLATION_LEVEL = "write.update.isolation-level";
  public static final String UPDATE_ISOLATION_LEVEL_DEFAULT = "serializable";

  public static final String UPDATE_MODE = "write.update.mode";
  public static final String UPDATE_MODE_DEFAULT = RowLevelOperationMode.COPY_ON_WRITE.modeName();

  public static final String UPDATE_DISTRIBUTION_MODE = "write.update.distribution-mode";

  public static final String MERGE_ISOLATION_LEVEL = "write.merge.isolation-level";
  public static final String MERGE_ISOLATION_LEVEL_DEFAULT = "serializable";

  public static final String MERGE_MODE = "write.merge.mode";
  public static final String MERGE_MODE_DEFAULT = RowLevelOperationMode.COPY_ON_WRITE.modeName();

  public static final String MERGE_DISTRIBUTION_MODE = "write.merge.distribution-mode";

  public static final String UPSERT_ENABLED = "write.upsert.enabled";
  public static final boolean UPSERT_ENABLED_DEFAULT = false;
}
