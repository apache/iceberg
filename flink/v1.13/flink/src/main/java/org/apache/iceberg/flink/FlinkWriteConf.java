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

import java.util.Locale;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;

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
 * The most specific value is set in write options and takes precedence over all other configs. If
 * no write option is provided, this class checks the flink configuration for any overrides. If no
 * applicable value is found in the write options, this class uses the table metadata.
 *
 * <p>Note this class is NOT meant to be serialized.
 */
public class FlinkWriteConf {

  private final FlinkConfParser confParser;

  public FlinkWriteConf(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
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
    return FileFormat.valueOf(valueAsString.toUpperCase(Locale.ENGLISH));
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

  public DistributionMode distributionMode() {
    String modeName =
        confParser
            .stringConf()
            .option(FlinkWriteOptions.DISTRIBUTION_MODE.key())
            .flinkConfig(FlinkWriteOptions.DISTRIBUTION_MODE)
            .tableProperty(TableProperties.WRITE_DISTRIBUTION_MODE)
            .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_NONE)
            .parse();
    return DistributionMode.fromName(modeName);
  }
}
