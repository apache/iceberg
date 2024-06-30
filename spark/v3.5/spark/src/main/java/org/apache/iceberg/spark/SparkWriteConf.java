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
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(SparkWriteConf.class);

  private static final long DATA_FILE_SIZE = 128 * 1024 * 1024; // 128 MB
  private static final long DELETE_FILE_SIZE = 32 * 1024 * 1024; // 32 MB

  private final SparkSession spark;
  private final Table table;
  private final String branch;
  private final RuntimeConfig sessionConf;
  private final Map<String, String> writeOptions;
  private final SparkConfParser confParser;

  public SparkWriteConf(SparkSession spark, Table table, Map<String, String> writeOptions) {
    this(spark, table, null, writeOptions);
  }

  public SparkWriteConf(
      SparkSession spark, Table table, String branch, Map<String, String> writeOptions) {
    this.spark = spark;
    this.table = table;
    this.branch = branch;
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
    return sessionConf.get(SparkSQLProperties.WAP_ID, null);
  }

  public boolean mergeSchema() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.MERGE_SCHEMA)
        .option(SparkWriteOptions.SPARK_MERGE_SCHEMA)
        .defaultValue(SparkWriteOptions.MERGE_SCHEMA_DEFAULT)
        .parse();
  }

  public int outputSpecId() {
    int outputSpecId =
        confParser
            .intConf()
            .option(SparkWriteOptions.OUTPUT_SPEC_ID)
            .defaultValue(table.spec().specId())
            .parse();
    Preconditions.checkArgument(
        table.specs().containsKey(outputSpecId),
        "Output spec id %s is not a valid spec id for table",
        outputSpecId);
    return outputSpecId;
  }

  public FileFormat dataFileFormat() {
    String valueAsString =
        confParser
            .stringConf()
            .option(SparkWriteOptions.WRITE_FORMAT)
            .tableProperty(TableProperties.DEFAULT_FILE_FORMAT)
            .defaultValue(TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)
            .parse();
    return FileFormat.fromString(valueAsString);
  }

  private String dataCompressionCodec() {
    switch (dataFileFormat()) {
      case PARQUET:
        return parquetCompressionCodec();
      case AVRO:
        return avroCompressionCodec();
      case ORC:
        return orcCompressionCodec();
      default:
        return null;
    }
  }

  public long targetDataFileSize() {
    return confParser
        .longConf()
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES)
        .tableProperty(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
        .defaultValue(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
        .parse();
  }

  public boolean useFanoutWriter(SparkWriteRequirements writeRequirements) {
    boolean defaultValue = !writeRequirements.hasOrdering();
    return fanoutWriterEnabled(defaultValue);
  }

  private boolean fanoutWriterEnabled() {
    return fanoutWriterEnabled(true /* enabled by default */);
  }

  private boolean fanoutWriterEnabled(boolean defaultValue) {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.FANOUT_ENABLED)
        .tableProperty(TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED)
        .defaultValue(defaultValue)
        .parse();
  }

  public FileFormat deleteFileFormat() {
    String valueAsString =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DELETE_FORMAT)
            .tableProperty(TableProperties.DELETE_DEFAULT_FILE_FORMAT)
            .parseOptional();
    return valueAsString != null ? FileFormat.fromString(valueAsString) : dataFileFormat();
  }

  private String deleteCompressionCodec() {
    switch (deleteFileFormat()) {
      case PARQUET:
        return deleteParquetCompressionCodec();
      case AVRO:
        return deleteAvroCompressionCodec();
      case ORC:
        return deleteOrcCompressionCodec();
      default:
        return null;
    }
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

  public SparkWriteRequirements writeRequirements() {
    if (ignoreTableDistributionAndOrdering()) {
      LOG.info("Skipping distribution/ordering: disabled per job configuration");
      return SparkWriteRequirements.EMPTY;
    }

    return SparkWriteUtil.writeRequirements(
        table, distributionMode(), fanoutWriterEnabled(), dataAdvisoryPartitionSize());
  }

  @VisibleForTesting
  DistributionMode distributionMode() {
    String modeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .sessionConf(SparkSQLProperties.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.WRITE_DISTRIBUTION_MODE)
            .parseOptional();

    if (modeName != null) {
      DistributionMode mode = DistributionMode.fromName(modeName);
      return adjustWriteDistributionMode(mode);
    } else {
      return defaultWriteDistributionMode();
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

  private DistributionMode defaultWriteDistributionMode() {
    if (table.sortOrder().isSorted()) {
      return RANGE;
    } else if (table.spec().isPartitioned()) {
      return HASH;
    } else {
      return NONE;
    }
  }

  public SparkWriteRequirements copyOnWriteRequirements(Command command) {
    if (ignoreTableDistributionAndOrdering()) {
      LOG.info("Skipping distribution/ordering: disabled per job configuration");
      return SparkWriteRequirements.EMPTY;
    }

    return SparkWriteUtil.copyOnWriteRequirements(
        table,
        command,
        copyOnWriteDistributionMode(command),
        fanoutWriterEnabled(),
        dataAdvisoryPartitionSize());
  }

  @VisibleForTesting
  DistributionMode copyOnWriteDistributionMode(Command command) {
    switch (command) {
      case DELETE:
        return deleteDistributionMode();
      case UPDATE:
        return updateDistributionMode();
      case MERGE:
        return copyOnWriteMergeDistributionMode();
      default:
        throw new IllegalArgumentException("Unexpected command: " + command);
    }
  }

  public SparkWriteRequirements positionDeltaRequirements(Command command) {
    if (ignoreTableDistributionAndOrdering()) {
      LOG.info("Skipping distribution/ordering: disabled per job configuration");
      return SparkWriteRequirements.EMPTY;
    }

    return SparkWriteUtil.positionDeltaRequirements(
        table,
        command,
        positionDeltaDistributionMode(command),
        fanoutWriterEnabled(),
        command == DELETE ? deleteAdvisoryPartitionSize() : dataAdvisoryPartitionSize());
  }

  @VisibleForTesting
  DistributionMode positionDeltaDistributionMode(Command command) {
    switch (command) {
      case DELETE:
        return deleteDistributionMode();
      case UPDATE:
        return updateDistributionMode();
      case MERGE:
        return positionDeltaMergeDistributionMode();
      default:
        throw new IllegalArgumentException("Unexpected command: " + command);
    }
  }

  private DistributionMode deleteDistributionMode() {
    String deleteModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .sessionConf(SparkSQLProperties.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.DELETE_DISTRIBUTION_MODE)
            .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_HASH)
            .parse();
    return DistributionMode.fromName(deleteModeName);
  }

  private DistributionMode updateDistributionMode() {
    String updateModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .sessionConf(SparkSQLProperties.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.UPDATE_DISTRIBUTION_MODE)
            .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_HASH)
            .parse();
    return DistributionMode.fromName(updateModeName);
  }

  private DistributionMode copyOnWriteMergeDistributionMode() {
    String mergeModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .sessionConf(SparkSQLProperties.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.MERGE_DISTRIBUTION_MODE)
            .parseOptional();

    if (mergeModeName != null) {
      DistributionMode mergeMode = DistributionMode.fromName(mergeModeName);
      return adjustWriteDistributionMode(mergeMode);

    } else if (table.spec().isPartitioned()) {
      return HASH;

    } else {
      return distributionMode();
    }
  }

  private DistributionMode positionDeltaMergeDistributionMode() {
    String mergeModeName =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DISTRIBUTION_MODE)
            .sessionConf(SparkSQLProperties.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.MERGE_DISTRIBUTION_MODE)
            .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_HASH)
            .parse();
    return DistributionMode.fromName(mergeModeName);
  }

  private boolean ignoreTableDistributionAndOrdering() {
    return confParser
        .booleanConf()
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING)
        .defaultValue(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING_DEFAULT)
        .negate()
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

  public boolean caseSensitive() {
    return confParser
        .booleanConf()
        .sessionConf(SQLConf.CASE_SENSITIVE().key())
        .defaultValue(SQLConf.CASE_SENSITIVE().defaultValueString())
        .parse();
  }

  public String branch() {
    if (wapEnabled()) {
      String wapId = wapId();
      String wapBranch =
          confParser.stringConf().sessionConf(SparkSQLProperties.WAP_BRANCH).parseOptional();

      ValidationException.check(
          wapId == null || wapBranch == null,
          "Cannot set both WAP ID and branch, but got ID [%s] and branch [%s]",
          wapId,
          wapBranch);

      if (wapBranch != null) {
        ValidationException.check(
            branch == null,
            "Cannot write to both branch and WAP branch, but got branch [%s] and WAP branch [%s]",
            branch,
            wapBranch);

        return wapBranch;
      }
    }

    return branch;
  }

  public Map<String, String> writeProperties() {
    Map<String, String> writeProperties = Maps.newHashMap();
    writeProperties.putAll(dataWriteProperties());
    writeProperties.putAll(deleteWriteProperties());
    return writeProperties;
  }

  private Map<String, String> dataWriteProperties() {
    Map<String, String> writeProperties = Maps.newHashMap();
    FileFormat dataFormat = dataFileFormat();

    switch (dataFormat) {
      case PARQUET:
        writeProperties.put(PARQUET_COMPRESSION, parquetCompressionCodec());
        String parquetCompressionLevel = parquetCompressionLevel();
        if (parquetCompressionLevel != null) {
          writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
        }
        break;

      case AVRO:
        writeProperties.put(AVRO_COMPRESSION, avroCompressionCodec());
        String avroCompressionLevel = avroCompressionLevel();
        if (avroCompressionLevel != null) {
          writeProperties.put(AVRO_COMPRESSION_LEVEL, avroCompressionLevel);
        }
        break;

      case ORC:
        writeProperties.put(ORC_COMPRESSION, orcCompressionCodec());
        writeProperties.put(ORC_COMPRESSION_STRATEGY, orcCompressionStrategy());
        break;

      default:
        // skip
    }

    return writeProperties;
  }

  private Map<String, String> deleteWriteProperties() {
    Map<String, String> writeProperties = Maps.newHashMap();
    FileFormat deleteFormat = deleteFileFormat();

    switch (deleteFormat) {
      case PARQUET:
        writeProperties.put(DELETE_PARQUET_COMPRESSION, deleteParquetCompressionCodec());
        String deleteParquetCompressionLevel = deleteParquetCompressionLevel();
        if (deleteParquetCompressionLevel != null) {
          writeProperties.put(DELETE_PARQUET_COMPRESSION_LEVEL, deleteParquetCompressionLevel);
        }
        break;

      case AVRO:
        writeProperties.put(DELETE_AVRO_COMPRESSION, deleteAvroCompressionCodec());
        String deleteAvroCompressionLevel = deleteAvroCompressionLevel();
        if (deleteAvroCompressionLevel != null) {
          writeProperties.put(DELETE_AVRO_COMPRESSION_LEVEL, deleteAvroCompressionLevel);
        }
        break;

      case ORC:
        writeProperties.put(DELETE_ORC_COMPRESSION, deleteOrcCompressionCodec());
        writeProperties.put(DELETE_ORC_COMPRESSION_STRATEGY, deleteOrcCompressionStrategy());
        break;

      default:
        // skip
    }

    return writeProperties;
  }

  private String parquetCompressionCodec() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_CODEC)
        .sessionConf(SparkSQLProperties.COMPRESSION_CODEC)
        .tableProperty(TableProperties.PARQUET_COMPRESSION)
        .defaultValue(TableProperties.PARQUET_COMPRESSION_DEFAULT)
        .parse();
  }

  private String deleteParquetCompressionCodec() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_CODEC)
        .sessionConf(SparkSQLProperties.COMPRESSION_CODEC)
        .tableProperty(DELETE_PARQUET_COMPRESSION)
        .defaultValue(parquetCompressionCodec())
        .parse();
  }

  private String parquetCompressionLevel() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_LEVEL)
        .sessionConf(SparkSQLProperties.COMPRESSION_LEVEL)
        .tableProperty(TableProperties.PARQUET_COMPRESSION_LEVEL)
        .defaultValue(TableProperties.PARQUET_COMPRESSION_LEVEL_DEFAULT)
        .parseOptional();
  }

  private String deleteParquetCompressionLevel() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_LEVEL)
        .sessionConf(SparkSQLProperties.COMPRESSION_LEVEL)
        .tableProperty(DELETE_PARQUET_COMPRESSION_LEVEL)
        .defaultValue(parquetCompressionLevel())
        .parseOptional();
  }

  private String avroCompressionCodec() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_CODEC)
        .sessionConf(SparkSQLProperties.COMPRESSION_CODEC)
        .tableProperty(TableProperties.AVRO_COMPRESSION)
        .defaultValue(TableProperties.AVRO_COMPRESSION_DEFAULT)
        .parse();
  }

  private String deleteAvroCompressionCodec() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_CODEC)
        .sessionConf(SparkSQLProperties.COMPRESSION_CODEC)
        .tableProperty(DELETE_AVRO_COMPRESSION)
        .defaultValue(avroCompressionCodec())
        .parse();
  }

  private String avroCompressionLevel() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_LEVEL)
        .sessionConf(SparkSQLProperties.COMPRESSION_LEVEL)
        .tableProperty(TableProperties.AVRO_COMPRESSION_LEVEL)
        .defaultValue(TableProperties.AVRO_COMPRESSION_LEVEL_DEFAULT)
        .parseOptional();
  }

  private String deleteAvroCompressionLevel() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_LEVEL)
        .sessionConf(SparkSQLProperties.COMPRESSION_LEVEL)
        .tableProperty(DELETE_AVRO_COMPRESSION_LEVEL)
        .defaultValue(avroCompressionLevel())
        .parseOptional();
  }

  private String orcCompressionCodec() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_CODEC)
        .sessionConf(SparkSQLProperties.COMPRESSION_CODEC)
        .tableProperty(TableProperties.ORC_COMPRESSION)
        .defaultValue(TableProperties.ORC_COMPRESSION_DEFAULT)
        .parse();
  }

  private String deleteOrcCompressionCodec() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_CODEC)
        .sessionConf(SparkSQLProperties.COMPRESSION_CODEC)
        .tableProperty(DELETE_ORC_COMPRESSION)
        .defaultValue(orcCompressionCodec())
        .parse();
  }

  private String orcCompressionStrategy() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_STRATEGY)
        .sessionConf(SparkSQLProperties.COMPRESSION_STRATEGY)
        .tableProperty(TableProperties.ORC_COMPRESSION_STRATEGY)
        .defaultValue(TableProperties.ORC_COMPRESSION_STRATEGY_DEFAULT)
        .parse();
  }

  private String deleteOrcCompressionStrategy() {
    return confParser
        .stringConf()
        .option(SparkWriteOptions.COMPRESSION_STRATEGY)
        .sessionConf(SparkSQLProperties.COMPRESSION_STRATEGY)
        .tableProperty(DELETE_ORC_COMPRESSION_STRATEGY)
        .defaultValue(orcCompressionStrategy())
        .parse();
  }

  private long dataAdvisoryPartitionSize() {
    long defaultValue =
        advisoryPartitionSize(DATA_FILE_SIZE, dataFileFormat(), dataCompressionCodec());
    return advisoryPartitionSize(defaultValue);
  }

  private long deleteAdvisoryPartitionSize() {
    long defaultValue =
        advisoryPartitionSize(DELETE_FILE_SIZE, deleteFileFormat(), deleteCompressionCodec());
    return advisoryPartitionSize(defaultValue);
  }

  private long advisoryPartitionSize(long defaultValue) {
    return confParser
        .longConf()
        .option(SparkWriteOptions.ADVISORY_PARTITION_SIZE)
        .sessionConf(SparkSQLProperties.ADVISORY_PARTITION_SIZE)
        .tableProperty(TableProperties.SPARK_WRITE_ADVISORY_PARTITION_SIZE_BYTES)
        .defaultValue(defaultValue)
        .parse();
  }

  private long advisoryPartitionSize(
      long expectedFileSize, FileFormat outputFileFormat, String outputCodec) {
    double shuffleCompressionRatio = shuffleCompressionRatio(outputFileFormat, outputCodec);
    long suggestedAdvisoryPartitionSize = (long) (expectedFileSize * shuffleCompressionRatio);
    return Math.max(suggestedAdvisoryPartitionSize, sparkAdvisoryPartitionSize());
  }

  private long sparkAdvisoryPartitionSize() {
    return (long) spark.sessionState().conf().getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES());
  }

  private double shuffleCompressionRatio(FileFormat outputFileFormat, String outputCodec) {
    return SparkCompressionUtil.shuffleCompressionRatio(spark, outputFileFormat, outputCodec);
  }

  public DeleteGranularity deleteGranularity() {
    String valueAsString =
        confParser
            .stringConf()
            .option(SparkWriteOptions.DELETE_GRANULARITY)
            .tableProperty(TableProperties.DELETE_GRANULARITY)
            .defaultValue(TableProperties.DELETE_GRANULARITY_DEFAULT)
            .parse();
    return DeleteGranularity.fromString(valueAsString);
  }
}
