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

package com.netflix.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.types.CheckCompatibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister {

  private SparkSession lazySpark = null;
  private Configuration lazyConf = null;

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    Configuration conf = mergeIcebergHadoopConfs(lazyBaseConf(), options.asMap());
    Table table = findTable(options, conf);
    Configuration withTableConfs = mergeIcebergHadoopConfs(conf, table.properties());
    return new Reader(table, withTableConfs);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType dfStruct, SaveMode mode,
                                                   DataSourceOptions options) {
    Preconditions.checkArgument(mode == SaveMode.Append, "Save mode %s is not supported", mode);
    Configuration conf = mergeIcebergHadoopConfs(lazyBaseConf(), options.asMap());
    Table table = findTable(options, conf);
    Configuration withTableHadoopConfs = mergeIcebergHadoopConfs(conf, table.properties());

    Schema dfSchema = SparkSchemaUtil.convert(table.schema(), dfStruct);
    List<String> errors = CheckCompatibility.writeCompatibilityErrors(table.schema(), dfSchema);
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Cannot write incompatible dataframe to table with schema:\n")
          .append(table.schema()).append("\nProblems:");
      for (String error : errors) {
        sb.append("\n* ").append(error);
      }
      throw new IllegalArgumentException(sb.toString());
    }

    Optional<String> formatOption = options.get("iceberg.write.format");
    FileFormat format;
    if (formatOption.isPresent()) {
      format = FileFormat.valueOf(formatOption.get().toUpperCase(Locale.ENGLISH));
    } else {
      format = FileFormat.valueOf(table.properties()
          .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
          .toUpperCase(Locale.ENGLISH));
    }

    return Optional.of(new Writer(table, withTableHadoopConfs, format));
  }

  protected Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> location = options.get("path");
    Preconditions.checkArgument(location.isPresent(),
        "Cannot open table without a location: path is not set");

    HadoopTables tables = new HadoopTables(conf);

    return tables.load(location.get());
  }

  protected SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  protected Configuration lazyBaseConf() {
    if (lazyConf == null) {
      this.lazyConf = lazySparkSession().sparkContext().hadoopConfiguration();
    }
    return lazyConf;
  }

  protected Configuration mergeIcebergHadoopConfs(Configuration baseConf, Map<String, String> options) {
    Configuration resolvedConf = new Configuration(baseConf);
    options.keySet().stream()
        .filter(key -> key.startsWith("iceberg.hadoop"))
        .filter(key -> baseConf.get(key) == null)
        .forEach(key -> resolvedConf.set(key.replaceFirst("iceberg.hadoop", ""), options.get(key)));
    return resolvedConf;
  }
}
