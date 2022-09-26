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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;

public class TableMigrationUtil {
  private static final PathFilter HIDDEN_PATH_FILTER =
      p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  private TableMigrationUtil() {}

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * <p>For Parquet and ORC partitions, this will read metrics from the file footer. For Avro
   * partitions, metrics are set to null.
   *
   * <p>Note: certain metrics, like NaN counts, that are only supported by iceberg file writers but
   * not file footers, will not be populated.
   *
   * @param partition partition key, e.g., "a=1/b=2"
   * @param uri partition location URI
   * @param format partition format, avro, parquet or orc
   * @param spec a partition spec
   * @param conf a Hadoop conf
   * @param metricsConfig a metrics conf
   * @param mapping a name mapping
   * @return a List of DataFile
   */
  public static List<DataFile> listPartition(
      Map<String, String> partition,
      String uri,
      String format,
      PartitionSpec spec,
      Configuration conf,
      MetricsConfig metricsConfig,
      NameMapping mapping) {
    return listPartition(partition, uri, format, spec, conf, metricsConfig, mapping, 1);
  }

  public static List<DataFile> listPartition(
      Map<String, String> partitionPath,
      String partitionUri,
      String format,
      PartitionSpec spec,
      Configuration conf,
      MetricsConfig metricsSpec,
      NameMapping mapping,
      int parallelism) {
    ExecutorService service = null;
    try {
      String partitionKey =
          spec.fields().stream()
              .map(PartitionField::name)
              .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
              .collect(Collectors.joining("/"));

      Path partition = new Path(partitionUri);
      FileSystem fs = partition.getFileSystem(conf);
      List<FileStatus> fileStatus =
          Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
              .filter(FileStatus::isFile)
              .collect(Collectors.toList());
      DataFile[] datafiles = new DataFile[fileStatus.size()];
      Tasks.Builder<Integer> task =
          Tasks.range(fileStatus.size()).stopOnFailure().throwFailureWhenFinished();

      if (parallelism > 1) {
        service = migrationService(parallelism);
        task.executeWith(service);
      }

      if (format.contains("avro")) {
        task.run(
            index -> {
              Metrics metrics = getAvroMetrics(fileStatus.get(index).getPath(), conf);
              datafiles[index] =
                  buildDataFile(fileStatus.get(index), partitionKey, spec, metrics, "avro");
            });
      } else if (format.contains("parquet")) {
        task.run(
            index -> {
              Metrics metrics =
                  getParquetMetrics(fileStatus.get(index).getPath(), conf, metricsSpec, mapping);
              datafiles[index] =
                  buildDataFile(fileStatus.get(index), partitionKey, spec, metrics, "parquet");
            });
      } else if (format.contains("orc")) {
        task.run(
            index -> {
              Metrics metrics =
                  getOrcMetrics(fileStatus.get(index).getPath(), conf, metricsSpec, mapping);
              datafiles[index] =
                  buildDataFile(fileStatus.get(index), partitionKey, spec, metrics, "orc");
            });
      } else {
        throw new UnsupportedOperationException("Unknown partition format: " + format);
      }
      return Arrays.asList(datafiles);
    } catch (IOException e) {
      throw new RuntimeException("Unable to list files in partition: " + partitionUri, e);
    } finally {
      if (service != null) {
        service.shutdown();
      }
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

  private static DataFile buildDataFile(
      FileStatus stat, String partitionKey, PartitionSpec spec, Metrics metrics, String format) {
    return DataFiles.builder(spec)
        .withPath(stat.getPath().toString())
        .withFormat(format)
        .withFileSizeInBytes(stat.getLen())
        .withMetrics(metrics)
        .withPartitionPath(partitionKey)
        .build();
  }

  private static ExecutorService migrationService(int concurrentDeletes) {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                concurrentDeletes,
                new ThreadFactoryBuilder().setNameFormat("table-migration-%d").build()));
  }
}
