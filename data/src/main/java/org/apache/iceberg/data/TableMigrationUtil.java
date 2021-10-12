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

package org.apache.iceberg.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class TableMigrationUtil {

  private static final PathFilter HIDDEN_PATH_FILTER =
      p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  private TableMigrationUtil() {
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   * <p>
   * For Parquet and ORC partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   * <p>
   * Note: certain metrics, like NaN counts, that are only supported by iceberg file writers but not file footers,
   * will not be populated.
   *
   * @param partition partition key, e.g., "a=1/b=2"
   * @param uri           partition location URI
   * @param format        partition format, avro, parquet or orc
   * @param spec          a partition spec
   * @param conf          a Hadoop conf
   * @param metricsConfig a metrics conf
   * @param mapping       a name mapping
   * @return a List of DataFile
   */
  public static List<DataFile> listPartition(Map<String, String> partition, String uri, String format,
                                             PartitionSpec spec, Configuration conf, MetricsConfig metricsConfig,
                                             NameMapping mapping) {
    if (format.contains("avro")) {
      return listAvroPartition(partition, uri, spec, conf);
    } else if (format.contains("parquet")) {
      return listParquetPartition(partition, uri, spec, conf, metricsConfig, mapping);
    } else if (format.contains("orc")) {
      return listOrcPartition(partition, uri, spec, conf, metricsConfig, mapping);
    } else {
      throw new UnsupportedOperationException("Unknown partition format: " + format);
    }
  }

  private static List<DataFile> listAvroPartition(Map<String, String> partitionPath, String partitionUri,
                                                  PartitionSpec spec, Configuration conf) {
    try {
      Path partition = new Path(partitionUri);
      FileSystem fs = partition.getFileSystem(conf);
      return Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
          .filter(FileStatus::isFile)
          .map(stat -> {
            Metrics metrics = new Metrics(-1L, null, null, null);
            String partitionKey = spec.fields().stream()
                .map(PartitionField::name)
                .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                .collect(Collectors.joining("/"));

            return DataFiles.builder(spec)
                .withPath(stat.getPath().toString())
                .withFormat("avro")
                .withFileSizeInBytes(stat.getLen())
                .withMetrics(metrics)
                .withPartitionPath(partitionKey)
                .build();

          }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Unable to list files in partition: " + partitionUri, e);
    }
  }

  private static List<DataFile> listParquetPartition(Map<String, String> partitionPath, String partitionUri,
                                                     PartitionSpec spec, Configuration conf,
                                                     MetricsConfig metricsSpec, NameMapping mapping) {
    try {
      Path partition = new Path(partitionUri);
      FileSystem fs = partition.getFileSystem(conf);

      return Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
          .filter(FileStatus::isFile)
          .map(stat -> {
            Metrics metrics;
            try {
              ParquetMetadata metadata = ParquetFileReader.readFooter(conf, stat);
              metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsSpec, mapping);
            } catch (IOException e) {
              throw new RuntimeException("Unable to read the footer of the parquet file: " +
                  stat.getPath(), e);
            }
            String partitionKey = spec.fields().stream()
                .map(PartitionField::name)
                .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                .collect(Collectors.joining("/"));

            return DataFiles.builder(spec)
                .withPath(stat.getPath().toString())
                .withFormat("parquet")
                .withFileSizeInBytes(stat.getLen())
                .withMetrics(metrics)
                .withPartitionPath(partitionKey)
                .build();
          }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Unable to list files in partition: " + partitionUri, e);
    }
  }

  private static List<DataFile> listOrcPartition(Map<String, String> partitionPath, String partitionUri,
                                                 PartitionSpec spec, Configuration conf,
                                                 MetricsConfig metricsSpec, NameMapping mapping) {
    try {
      Path partition = new Path(partitionUri);
      FileSystem fs = partition.getFileSystem(conf);

      return Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
          .filter(FileStatus::isFile)
          .map(stat -> {
            Metrics metrics = OrcMetrics.fromInputFile(HadoopInputFile.fromPath(stat.getPath(), conf),
                metricsSpec, mapping);
            String partitionKey = spec.fields().stream()
                .map(PartitionField::name)
                .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                .collect(Collectors.joining("/"));

            return DataFiles.builder(spec)
                .withPath(stat.getPath().toString())
                .withFormat("orc")
                .withFileSizeInBytes(stat.getLen())
                .withMetrics(metrics)
                .withPartitionPath(partitionKey)
                .build();

          }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Unable to list files in partition: " + partitionUri, e);
    }
  }
}
