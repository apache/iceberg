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
package org.apache.iceberg.delta.utils;

import java.io.UncheckedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;

public class FileMetricsReader {

  private FileMetricsReader() {}

  public static Metrics getMetricsForFile(
      Table table, String fullFilePath, FileFormat format, Configuration hadoopConfiguration) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

    switch (format) {
      case PARQUET:
        return getParquetMetrics(
            new Path(fullFilePath), hadoopConfiguration, metricsConfig, nameMapping);
      case AVRO:
        return getAvroMetrics(new Path(fullFilePath), hadoopConfiguration);
      case ORC:
        return getOrcMetrics(
            new Path(fullFilePath), hadoopConfiguration, metricsConfig, nameMapping);
      default:
        throw new ValidationException("Unsupported file format: %s", format);
    }
  }

  private static Metrics getAvroMetrics(Path path, Configuration conf) {
    try {
      InputFile file = HadoopInputFile.fromPath(path, conf);
      long rowCount = Avro.rowCount(file);
      return new Metrics(rowCount, null, null, null, null);
    } catch (UncheckedIOException e) {
      throw new RuntimeException("Unable to read Avro file: " + path, e);
    }
  }

  private static Metrics getParquetMetrics(
      Path path, Configuration conf, MetricsConfig metricsSpec, NameMapping mapping) {
    try {
      InputFile file = HadoopInputFile.fromPath(path, conf);
      return ParquetUtil.fileMetrics(file, metricsSpec, mapping);
    } catch (UncheckedIOException e) {
      throw new RuntimeException("Unable to read the metrics of the Parquet file: " + path, e);
    }
  }

  private static Metrics getOrcMetrics(
      Path path, Configuration conf, MetricsConfig metricsSpec, NameMapping mapping) {
    try {
      return OrcMetrics.fromInputFile(HadoopInputFile.fromPath(path, conf), metricsSpec, mapping);
    } catch (UncheckedIOException e) {
      throw new RuntimeException("Unable to read the metrics of the Orc file: " + path, e);
    }
  }
}
