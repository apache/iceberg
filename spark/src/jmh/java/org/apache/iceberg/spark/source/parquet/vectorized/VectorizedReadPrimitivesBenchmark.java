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

package org.apache.iceberg.spark.source.parquet.vectorized;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.when;

public class VectorizedReadPrimitivesBenchmark extends VectorizedIcebergSourceBenchmark {

  @Override
  protected final Table initTable() {
    Schema schema = new Schema(
        optional(1, "longCol", Types.LongType.get()),
        optional(2, "intCol", Types.LongType.get()),
        optional(3, "floatCol", Types.LongType.get()),
        optional(4, "doubleCol", Types.LongType.get()),
        optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
        optional(6, "dateCol", Types.DateType.get()),
        optional(7, "timestampCol", Types.TimestampType.withZone()),
        optional(8, "stringCol", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    properties.put(TableProperties.PARQUET_DICT_SIZE_BYTES, "1");
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }

  @Override
  protected void appendData() {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df = spark().range(NUM_ROWS)
          .withColumn("longCol", when(pmod(col("id"), lit(2)).equalTo(lit(0)), lit(null)).otherwise(col("id")))
          .drop("id")
          .withColumn("intCol", expr("CAST(longCol AS BIGINT)"))
          .withColumn("floatCol", expr("CAST(longCol AS BIGINT)"))
          .withColumn("doubleCol", expr("CAST(longCol AS BIGINT)"))
          .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(20, 5))"))
          .withColumn("dateCol", date_add(current_date(), fileNum))
          .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
          .withColumn("stringCol", expr("CAST(longCol AS STRING)"));
      appendAsFile(df);
    }
  }
}
