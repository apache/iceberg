/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer;
import org.apache.spark.sql.types.StructType;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister {

  private Configuration lazyConf = null;

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public DataSourceV2Reader createReader(DataSourceV2Options options) {
    Table table = findTable(options);
    return new Reader(table, lazyConf());
  }

  @Override
  public Optional<DataSourceV2Writer> createWriter(String jobId, StructType dfStruct, SaveMode mode,
                                                   DataSourceV2Options options) {
    Preconditions.checkArgument(mode == SaveMode.Append, "Save mode %s is not supported", mode);

    Table table = findTable(options);

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

    return Optional.of(new Writer(table, lazyConf(), format));
  }

  protected Table findTable(DataSourceV2Options options) {
    Optional<String> location = options.get("path");
    Preconditions.checkArgument(location.isPresent(),
        "Cannot open table without a location: path is not set");

    HadoopTables tables = new HadoopTables(lazyConf());

    return tables.load(location.get());
  }

  protected Configuration lazyConf() {
    if (lazyConf == null) {
      SparkSession session = SparkSession.builder().getOrCreate();
      this.lazyConf = session.sparkContext().hadoopConfiguration();
    }
    return lazyConf;
  }
}
