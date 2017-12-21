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
import java.util.Locale;
import java.util.Optional;

import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister {

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public DataSourceV2Reader createReader(DataSourceV2Options options) {
    SparkSession session = SparkSession.builder().getOrCreate();
    Configuration conf = session.sparkContext().hadoopConfiguration();

    Optional<String> location = options.get("iceberg.table.location");
    if (location.isPresent()) {
      HadoopTables tables = new HadoopTables(conf);
      Table table = tables.load(location.get());
      return new Reader(table, location.get(), conf);
    }

    throw new IllegalArgumentException("Cannot open table without a location");
  }

  @Override
  public Optional<DataSourceV2Writer> createWriter(String jobId, StructType dfStruct, SaveMode mode,
                                                   DataSourceV2Options options) {
    Preconditions.checkArgument(mode == SaveMode.Append, "Save mode %s is not supported", mode);

    SparkSession session = SparkSession.builder().getOrCreate();
    Configuration conf = session.sparkContext().hadoopConfiguration();

    Optional<String> location = options.get("iceberg.table.location");
    if (location.isPresent()) {
      HadoopTables tables = new HadoopTables(conf);
      Table table = tables.load(location.get());

      // TODO: prune isn't quite correct. this should convert and fill in the right ids
      Schema dfSchema = SparkSchemaUtil.prune(table.schema(), dfStruct);
      // TODO: this constraint can be relaxed to table.schema().canRead(dfSchema)
      if (!dfSchema.asStruct().equals(table.schema().asStruct())) {
        throw new IllegalArgumentException(String.format(
            "Cannot write incompatible dataframe schema:\n%s\nTo table with schema:\n%s",
            dfSchema, table.schema()));
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

      return Optional.of(new Writer(table, location.get(), conf, format));
    }

    throw new IllegalArgumentException("Cannot open table without a location");
  }

}
