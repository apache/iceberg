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

import static org.apache.iceberg.DistributionMode.HASH;
import static org.apache.iceberg.DistributionMode.NONE;
import static org.apache.iceberg.DistributionMode.RANGE;

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;

/**
 * A class for common Iceberg configs for Spark writes.
 *
 * <p>If a config is set at multiple levels, the following order of precedence is used (top to
 * bottom):
 *
 * <ol>
 *   <li>Write options
 *   <li>Session configuration
 *   <li>Table metadata
 * </ol>
 *
 * The most specific value is set in write options and takes precedence over all other configs. If
 * no write option is provided, this class checks the session configuration for any overrides. If no
 * applicable value is found in the session configuration, this class uses the table metadata.
 *
 * <p>Note this class is NOT meant to be serialized and sent to executors.
 */
public class SparkWriteConf {

  private final Table table;
  private final RuntimeConfig sessionConf;
  private final Map<String, String> writeOptions;
  private final SparkConfParser confParser;

  public SparkWriteConf(SparkSession spark, Table table, Map<String, String> writeOptions) {
    this.table = table;
    this.sessionConf = spark.conf();
    this.writeOptions = writeOptions;
    this.confParser = new SparkConfParser(spark, table, writeOptions);
  }

  public boolean checkNullability() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.CHECK_NULLABILITY)
        .sessionConf(SparkSQLProperties.CHECK_NULLABILITY)
        .defaultValue(SparkSQLProperties.CHECK_NULLABILITY_DEFAULT)
        .parse();
  }

  public boolean checkOrdering() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.CHECK_ORDERING)
        .sessionConf(SparkSQLProperties.CHECK_ORDERING)
        .defaultValue(SparkSQLProperties.CHECK_ORDERING_DEFAULT)
        .parse();
  }

  /**
   * Enables writing a timestamp with time zone as a timestamp without time zone.
   *
   * <p>Generally, this is not safe as a timestamp without time zone is supposed to represent the
   * wall-clock time, i.e. no matter the reader/writer timezone 3PM should always be read as 3PM,
   * but a timestamp with time zone represents instant semantics, i.e. the timestamp is adjusted so
   * that the corresponding time in the reader timezone is displayed.
   *
   * <p>When set to false (default), an exception must be thrown if the table contains a timestamp
   * without time zone.
   *
   * @return boolean indicating if writing timestamps without timezone is allowed
   */
  public boolean handleTimestampWithoutZone() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)
        .sessionConf(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)
        .defaultValue(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT)
        .parse();
  }

  public String overwriteMode() {
    String overwriteMode = writeOptions.get(SparkWriteOptions.OVERWRITE_MODE);
    return overwriteMode != null ? overwriteMode.toLowerCase(Locale.ROOT) : null;
  }

  public boolean wapEnabled() {
    return confParser
        .booleanConf()
        .tableProperty(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED)
        .defaultValue(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT)
        .parse();
  }

  public String wapId() {
    return sessionConf.get("spark.wap.id", null);
  }

  public boolean mergeSchema() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.MERGE_SCHEMA)
        .option(SparkWriteOptions.SPARK_MERGE_SCHEMA)
        .defaultValue(SparkWriteOptions.MERGE_SCHEMA_DEFAULT)
        .parse();
  }

  public FileFormat dataFileFormat() {
    String valueAsString =
        confParser
            .stringConf()
            .option(SparkWriteOptions.WRITE_FORMAT)
            .tableProperty(TableProperties.DEFAULT_FILE_FORMAT)
            .defaultValue(TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)
            .parse();
    return FileFormat.valueOf(valueAsString.toUpperCase(Locale.ENGLISH));
  }

  public long targetDataFileSize() {
    return confParser
        .longConf()
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES)
        .tableProperty(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
        .defaultValue(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
        .parse();
  }

  public boolean fanoutWriterEnabled() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.FANOUT_ENABLED)
        .tableProperty(TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED)
        .defaultValue(TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED_DEFAULT)
        .parse();
  }

  public FileFormat deleteFileFormat() {
    String valueAsString =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DELETE_FORMAT)
            .tableProperty(TableProperties.DELETE_DEFAULT_FILE_FORMAT)
            .parseOptional();
    return valueAsString != null
        ? FileFormat.valueOf(valueAsString.toUpperCase(Locale.ENGLISH))
        : dataFileFormat();
  }

  public long targetDeleteFileSize() {
    return confParser
        .longConf()
        .option(SparkWriteOptions.TARGET_DELETE_FILE_SIZE_BYTES)
        .tableProperty(TableProperties.DELETE_TARGET_FILE_SIZE_BYTES)
        .defaultValue(TableProperties.DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT)
        .parse();
  }

  public Map<String, String> extraSnapshotMetadata() {
    Map<String, String> extraSnapshotMetadata = Maps.newHashMap();

    writeOptions.forEach(
        (key, value) -> {
          if (key.startsWith(SnapshotSummary.EXTRA_METADATA_PREFIX)) {
            extraSnapshotMetadata.put(
                key.substring(SnapshotSummary.EXTRA_METADATA_PREFIX.length()), value);
          }
        });

    return extraSnapshotMetadata;
  }

  public String rewrittenFileSetId() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID)
        .parseOptional();
  }

  public DistributionMode distributionMode() {
    String modeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.WRITE_DISTRIBUTION_MODE)
            .parseOptional();

    if (modeName != null) {
      DistributionMode mode = DistributionMode.fromName(modeName);
      return adjustWriteDistributionMode(mode);
    } else {
      return table.sortOrder().isSorted() ? RANGE : NONE;
    }
  }

  private DistributionMode adjustWriteDistributionMode(DistributionMode mode) {
    if (mode == RANGE && table.spec().isUnpartitioned() && table.sortOrder().isUnsorted()) {
      return NONE;
    } else if (mode == HASH && table.spec().isUnpartitioned()) {
      return NONE;
    } else {
      return mode;
    }
  }

  public DistributionMode deleteDistributionMode() {
    String deleteModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.DELETE_DISTRIBUTION_MODE)
            .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_HASH)
            .parse();
    return DistributionMode.fromName(deleteModeName);
  }

  public DistributionMode updateDistributionMode() {
    String updateModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.UPDATE_DISTRIBUTION_MODE)
            .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_HASH)
            .parse();
    return DistributionMode.fromName(updateModeName);
  }

  public DistributionMode copyOnWriteMergeDistributionMode() {
    String mergeModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.MERGE_DISTRIBUTION_MODE)
            .parseOptional();

    if (mergeModeName != null) {
      DistributionMode mergeMode = DistributionMode.fromName(mergeModeName);
      return adjustWriteDistributionMode(mergeMode);
    } else {
      return distributionMode();
    }
  }

  public DistributionMode positionDeltaMergeDistributionMode() {
    String mergeModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.MERGE_DISTRIBUTION_MODE)
            .parseOptional();
    return mergeModeName != null ? DistributionMode.fromName(mergeModeName) : distributionMode();
  }

  public boolean useTableDistributionAndOrdering() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING)
        .defaultValue(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING_DEFAULT)
        .parse();
  }

  public Long validateFromSnapshotId() {
    return confParser
        .longConf()
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID)
        .parseOptional();
  }

  public IsolationLevel isolationLevel() {
    String isolationLevelName =
        confParser.stringConf().option(SparkWriteOptions.ISOLATION_LEVEL).parseOptional();
    return isolationLevelName != null ? IsolationLevel.fromName(isolationLevelName) : null;
  }
}
