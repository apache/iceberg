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
package org.apache.iceberg.flink;

import static org.apache.iceberg.DistributionMode.HASH;
import static org.apache.iceberg.DistributionMode.NONE;
import static org.apache.iceberg.DistributionMode.RANGE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for common Iceberg configs for Flink writes.
 *
 * <p>If a config is set at multiple levels, the following order of precedence is used (top to
 * bottom):
 *
 * <ol>
 *   <li>Write options
 *   <li>flink ReadableConfig
 *   <li>Table metadata
 * </ol>
 *
 * <p>The most specific value is set in write options and takes precedence over all other configs.
 * If no write option is provided, this class checks the flink configuration for any overrides. If
 * no applicable value is found in the write options, this class uses the table metadata.
 *
 * <p>Note this class is NOT meant to be serialized.
 */
public class FlinkWriteConf {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkWriteConf.class);

  private final FlinkConfParser confParser;
  private final Table table;

  public FlinkWriteConf(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
    this.table = table;
  }

  public boolean overwriteMode() {
    return confParser
        .booleanConf()
        .option(FlinkWriteOptions.OVERWRITE_MODE.key())
        .flinkConfig(FlinkWriteOptions.OVERWRITE_MODE)
        .defaultValue(FlinkWriteOptions.OVERWRITE_MODE.defaultValue())
        .parse();
  }

  public boolean upsertMode() {
    return confParser
        .booleanConf()
        .option(FlinkWriteOptions.WRITE_UPSERT_ENABLED.key())
        .flinkConfig(FlinkWriteOptions.WRITE_UPSERT_ENABLED)
        .tableProperty(TableProperties.UPSERT_ENABLED)
        .defaultValue(TableProperties.UPSERT_ENABLED_DEFAULT)
        .parse();
  }

  public FileFormat dataFileFormat() {
    String valueAsString =
        confParser
            .stringConf()
            .option(FlinkWriteOptions.WRITE_FORMAT.key())
            .flinkConfig(FlinkWriteOptions.WRITE_FORMAT)
            .tableProperty(TableProperties.DEFAULT_FILE_FORMAT)
            .defaultValue(TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)
            .parse();
    return FileFormat.fromString(valueAsString);
  }

  public long targetDataFileSize() {
    return confParser
        .longConf()
        .option(FlinkWriteOptions.TARGET_FILE_SIZE_BYTES.key())
        .flinkConfig(FlinkWriteOptions.TARGET_FILE_SIZE_BYTES)
        .tableProperty(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
        .defaultValue(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
        .parse();
  }

  public String parquetCompressionCodec() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.COMPRESSION_CODEC.key())
        .flinkConfig(FlinkWriteOptions.COMPRESSION_CODEC)
        .tableProperty(TableProperties.PARQUET_COMPRESSION)
        .defaultValue(TableProperties.PARQUET_COMPRESSION_DEFAULT)
        .parse();
  }

  public String parquetCompressionLevel() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.COMPRESSION_LEVEL.key())
        .flinkConfig(FlinkWriteOptions.COMPRESSION_LEVEL)
        .tableProperty(TableProperties.PARQUET_COMPRESSION_LEVEL)
        .defaultValue(TableProperties.PARQUET_COMPRESSION_LEVEL_DEFAULT)
        .parseOptional();
  }

  public String avroCompressionCodec() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.COMPRESSION_CODEC.key())
        .flinkConfig(FlinkWriteOptions.COMPRESSION_CODEC)
        .tableProperty(TableProperties.AVRO_COMPRESSION)
        .defaultValue(TableProperties.AVRO_COMPRESSION_DEFAULT)
        .parse();
  }

  public String avroCompressionLevel() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.COMPRESSION_LEVEL.key())
        .flinkConfig(FlinkWriteOptions.COMPRESSION_LEVEL)
        .tableProperty(TableProperties.AVRO_COMPRESSION_LEVEL)
        .defaultValue(TableProperties.AVRO_COMPRESSION_LEVEL_DEFAULT)
        .parseOptional();
  }

  public String orcCompressionCodec() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.COMPRESSION_CODEC.key())
        .flinkConfig(FlinkWriteOptions.COMPRESSION_CODEC)
        .tableProperty(TableProperties.ORC_COMPRESSION)
        .defaultValue(TableProperties.ORC_COMPRESSION_DEFAULT)
        .parse();
  }

  public String orcCompressionStrategy() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.COMPRESSION_STRATEGY.key())
        .flinkConfig(FlinkWriteOptions.COMPRESSION_STRATEGY)
        .tableProperty(TableProperties.ORC_COMPRESSION_STRATEGY)
        .defaultValue(TableProperties.ORC_COMPRESSION_STRATEGY_DEFAULT)
        .parse();
  }

  public DistributionMode distributionMode() {
    String modeName =
        confParser
            .stringConf()
            .option(FlinkWriteOptions.DISTRIBUTION_MODE.key())
            .flinkConfig(FlinkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.WRITE_DISTRIBUTION_MODE)
            .parseOptional();

    List<Integer> equalityFieldIds = equalityFieldIds();

    DistributionMode writeMode =
        modeName != null
            ? DistributionMode.fromName(modeName)
            : defaultWriteDistributionMode(equalityFieldIds);
    switch (writeMode) {
      case NONE:
        if (equalityFieldIds.isEmpty()) {
          return NONE;
        } else {
          LOG.warn("Switch to use 'hash' distribution mode, because there are equality fields set");
          return HASH;
        }

      case HASH:
        PartitionSpec partitionSpec = table.spec();
        if (equalityFieldIds.isEmpty()) {
          if (partitionSpec.isUnpartitioned()) {
            LOG.warn(
                "Fallback to use 'none' distribution mode, because there are no equality fields set and table is unpartitioned");
            return NONE;
          }
        } else {
          if (partitionSpec.isPartitioned()) {
            for (PartitionField partitionField : partitionSpec.fields()) {
              Preconditions.checkState(
                  equalityFieldIds.contains(partitionField.sourceId()),
                  "In 'hash' distribution mode with equality fields set, partition field '%s' "
                      + "should be included in equality fields: '%s'",
                  partitionField,
                  equalityFieldColumns());
            }
          }
        }
        return HASH;

      case RANGE:
        if (equalityFieldIds.isEmpty()) {
          LOG.warn(
              "Fallback to use 'none' distribution mode, because there are no equality fields set "
                  + "and {}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return NONE;
        } else {
          LOG.warn(
              "Switch to use 'hash' distribution mode, because there are equality fields set "
                  + "and {}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return HASH;
        }

      default:
        throw new RuntimeException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + writeMode);
    }
  }

  private DistributionMode defaultWriteDistributionMode(List<Integer> equalityFieldIds) {
    if (table.sortOrder().isSorted()) {
      return RANGE;
    } else if (table.spec().isPartitioned()) {
      if (!equalityFieldIds.isEmpty()) {
        PartitionSpec partitionSpec = table.spec();
        for (PartitionField partitionField : partitionSpec.fields()) {
          if (!equalityFieldIds.contains(partitionField.sourceId())) {
            return NONE;
          }
        }
      }

      return HASH;
    } else {
      return NONE;
    }
  }

  public int workerPoolSize() {
    return confParser
        .intConf()
        .flinkConfig(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE)
        .defaultValue(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.defaultValue())
        .parse();
  }

  public String branch() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.BRANCH.key())
        .defaultValue(FlinkWriteOptions.BRANCH.defaultValue())
        .parse();
  }

  public String equalityFieldColumns() {
    return confParser
        .stringConf()
        .option(FlinkWriteOptions.EQUALITY_FIELD_COLUMNS.key())
        .parseOptional();
  }

  public List<Integer> equalityFieldIds() {
    String fieldColumns = equalityFieldColumns();
    if (fieldColumns == null) {
      return Lists.newArrayList(table.schema().identifierFieldIds());
    }

    List<String> equalityFieldColumns = Arrays.asList(fieldColumns.split(","));
    if (equalityFieldColumns.isEmpty()) {
      return Lists.newArrayList(table.schema().identifierFieldIds());
    }

    Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
    for (String column : equalityFieldColumns) {
      org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
      Preconditions.checkNotNull(
          field,
          "Missing required equality field column '%s' in table schema %s",
          column,
          table.schema());
      equalityFieldSet.add(field.fieldId());
    }

    if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
      LOG.warn(
          "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
              + " {}, use job specified equality field columns as the equality fields by default.",
          equalityFieldSet,
          table.schema().identifierFieldIds());
    }

    return Lists.newArrayList(equalityFieldSet);
  }
}
