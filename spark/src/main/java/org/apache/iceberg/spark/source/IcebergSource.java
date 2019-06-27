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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister, StreamWriteSupport {

  private SparkSession lazySpark = null;
  private Configuration lazyConf = null;

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    String caseSensitive = lazySparkSession().conf().get("spark.sql.caseSensitive", "true");

    return new Reader(table, Boolean.valueOf(caseSensitive), options);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType dsStruct, SaveMode mode,
                                                 DataSourceOptions options) {
    Preconditions.checkArgument(mode == SaveMode.Append, "Save mode %s is not supported", mode);
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    validateWriteSchema(table.schema(), dsStruct);
    FileFormat format = getFileFormat(table.properties(), options);
    return Optional.of(new Writer(table, format));
  }

  @Override
  public StreamWriter createStreamWriter(String runId, StructType dsStruct,
                                         OutputMode mode, DataSourceOptions options) {
    Preconditions.checkArgument(
        mode == OutputMode.Append() || mode == OutputMode.Complete(),
        "Output mode %s is not supported", mode);
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    validateWriteSchema(table.schema(), dsStruct);
    FileFormat format = getFileFormat(table.properties(), options);
    // Spark 2.4.x passes runId to createStreamWriter instead of real queryId,
    // so we fetch it directly from sparkContext to make writes idempotent
    String queryId = lazySparkSession().sparkContext().getLocalProperty(StreamExecution.QUERY_ID_KEY());
    return new StreamingWriter(table, format, queryId, mode);
  }

  protected Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> path = options.get("path");
    Preconditions.checkArgument(path.isPresent(), "Cannot open table: path is not set");

    if (path.get().contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path.get());
    } else {
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path.get());
      return hiveCatalog.loadTable(tableIdentifier);
    }
  }

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  private Configuration lazyBaseConf() {
    if (lazyConf == null) {
      this.lazyConf = lazySparkSession().sparkContext().hadoopConfiguration();
    }
    return lazyConf;
  }

  private Table getTableAndResolveHadoopConfiguration(
      DataSourceOptions options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options.asMap());
    Table table = findTable(options, conf);
    // Set confs from table properties
    mergeIcebergHadoopConfs(conf, table.properties());
    // Re-overwrite values set in options and table properties but were not in the environment.
    mergeIcebergHadoopConfs(conf, options.asMap());
    return table;
  }

  private static void mergeIcebergHadoopConfs(
      Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("hadoop."))
        .forEach(key -> baseConf.set(key.replaceFirst("hadoop.", ""), options.get(key)));
  }

  private FileFormat getFileFormat(Map<String, String> tableProperties, DataSourceOptions options) {
    Optional<String> formatOption = options.get("write-format");
    String format = formatOption.orElse(tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  private void validateWriteSchema(Schema tableSchema, StructType dsStruct) {
    Schema dsSchema = SparkSchemaUtil.convert(tableSchema, dsStruct);
    List<String> errors = CheckCompatibility.writeCompatibilityErrors(tableSchema, dsSchema);
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Cannot write incompatible dataset to table with schema:\n")
          .append(tableSchema)
          .append("\nProblems:");
      for (String error : errors) {
        sb.append("\n* ").append(error);
      }
      throw new IllegalArgumentException(sb.toString());
    }
  }
}
