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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class IcebergSourceNestedDataBenchmark extends IcebergSourceBenchmark {

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  @Override
  protected final Table initTable() {
    Schema schema = new Schema(
        required(0, "id", Types.LongType.get()),
        optional(4, "nested", Types.StructType.of(
            required(1, "col1", Types.StringType.get()),
            required(2, "col2", Types.DoubleType.get()),
            required(3, "col3", Types.LongType.get())
        ))
    );
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }
}
