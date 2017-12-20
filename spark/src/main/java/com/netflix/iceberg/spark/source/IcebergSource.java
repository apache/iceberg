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

import com.netflix.iceberg.Table;
import com.netflix.iceberg.hadoop.HadoopTables;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import java.util.Optional;

public class IcebergSource implements DataSourceV2, ReadSupport, DataSourceRegister {
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
}
