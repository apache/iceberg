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

package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkExceptionUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Helper methods to repair manifest files.
 * TODO- repair split offsets
 */
public class RepairManifestHelper {

  private RepairManifestHelper() {
    // Prevent Construction
  }

    /**
     * Given a DataFile information, return Metrics
     * @param format file format
     * @param status file status
     * @param conf Hadoop configuration
     * @param metricsSpec metrics configuration
     * @param mapping name mapping
     * @return metrics
     */
  private static Metrics getMetrics(FileFormat format, FileStatus status, Configuration conf,
                                    MetricsConfig metricsSpec, NameMapping mapping) {
    switch (format) {
      case AVRO:
        return new Metrics(-1L, null, null, null);
      case ORC:
        return OrcMetrics.fromInputFile(HadoopInputFile.fromPath(status.getPath(), conf),
          metricsSpec, mapping);
      case PARQUET:
        try {
          ParquetMetadata metadata = ParquetFileReader.readFooter(conf, status);
          return ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsSpec, mapping);
        } catch (IOException e) {
          throw SparkExceptionUtil.toUncheckedException(
            e, "Unable to read the footer of the parquet file: %s", status.getPath());
        }
      default:
        throw new UnsupportedOperationException("Unknown file format: " + format);
    }
  }

  /**
   * Diffs two DataFile for potential for repair
   * @return a set of fields in human-readable format that differ between these DataFiles
   */
  static Set<String> diff(DataFile first, DataFile second) {
    Set<String> result = new HashSet<>();
    if (first.fileSizeInBytes() != second.fileSizeInBytes()) {
      result.add("file_size_in_bytes");
    }
    if (first.recordCount() != second.recordCount()) {
      result.add("record_count");
    }
    if (!Objects.equals(first.columnSizes(), second.columnSizes())) {
      result.add("column_sizes");
    }
    if (!Objects.equals(first.valueCounts(), second.valueCounts())) {
      result.add("value_counts");
    }
    if (!Objects.equals(first.nullValueCounts(), second.nullValueCounts())) {
      result.add("null_value_counts");
    }
    if (!Objects.equals(first.nanValueCounts(), second.nanValueCounts())) {
      result.add("nan_value_counts");
    }
    if (!Objects.equals(first.lowerBounds(), second.lowerBounds())) {
      result.add("lower_bounds");
    }
    if (!Objects.equals(first.upperBounds(), second.upperBounds())) {
      result.add("upper_bounds");
    }
    return result;
  }

  /**
   * Given a data file pointer, return a repaired version if actual file information does not match.
   * @param file spark data file
   * @param spec user-specified spec
   * @param table table information
   * @param conf Hadoop configuration
   * @return A repaired DataFile if repair was done (file information did not match), or None if not
   */
  static Optional<RepairedDataFile> repairDataFile(SparkDataFile file,
                                                   Table table,
                                                   PartitionSpec spec,
                                                   Configuration conf) {
    DataFiles.Builder newDfBuilder = DataFiles.builder(spec).copy(file);
    Path path = new Path(file.path().toString());
    try {
      FileSystem fs = path.getFileSystem(conf);
      FileStatus status = fs.getFileStatus(path);
      newDfBuilder.withStatus(status);
      String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
      newDfBuilder.withMetrics(getMetrics(file.format(), status, conf,
          MetricsConfig.fromProperties(table.properties()), nameMapping));

      DataFile newFile = newDfBuilder.build();
      Set<String> diff = RepairManifestHelper.diff(file, newFile);
      if (diff.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(new RepairedDataFile(newFile, diff));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Represents a repaired DataFile
   */
  public static class RepairedDataFile {
    private DataFile dataFile;
    private Set<String> repairedFields;

    RepairedDataFile(DataFile df, Set<String> repairedFields) {
      Preconditions.checkNotNull(df, "Data file is null");
      Preconditions.checkNotNull(repairedFields, "Repaired fields is null");
      this.dataFile = df;
      this.repairedFields = repairedFields;
    }

    /**
     * @return a set of fields repaired in the Data File, or empty set if none repaired
     */
    Set<String> repairedFields() {
      return repairedFields;
    }

    /**
     * @return the repaired DataFile
     */
    DataFile dataFile() {
      return dataFile;
    }
  }

  /**
   * Represents a repaired manifest file
   */
  public static class RepairedManifestFile implements Serializable {
    private ManifestFile manifestFile;
    private Set<String> repairedFields;

    RepairedManifestFile(ManifestFile mf, Set<String> repairedFields) {
      Preconditions.checkNotNull(mf, "Manifest file is null");
      Preconditions.checkNotNull(repairedFields, "Repaired fields are null");
      this.manifestFile = mf;
      this.repairedFields = repairedFields;
    }

    /**
     * @return List of fields repaired of some entries in this manifest.
     */
    Set<String> repairedFields() {
      return repairedFields;
    }

    /**
     * @return Actual ManifestFile
     */
    ManifestFile manifestFile() {
      return manifestFile;
    }
  }
}
