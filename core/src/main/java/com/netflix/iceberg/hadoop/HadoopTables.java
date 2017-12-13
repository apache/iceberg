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

package com.netflix.iceberg.hadoop;

import com.netflix.iceberg.BaseTable;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.TableOperations;
import com.netflix.iceberg.exceptions.AlreadyExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HadoopTables {
  private final Configuration conf;

  public HadoopTables(Configuration conf) {
    this.conf = conf;
  }

  public Table load(String location) {
    return new BaseTable(newTableOps(location, conf), location);
  }

  public Table create(Schema schema, PartitionSpec spec, String location) {
    TableOperations ops = newTableOps(location, conf);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists at location: " + location);
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(ops, schema, spec);
    ops.commit(null, metadata);

    return new BaseTable(ops, location);
  }

  private TableOperations newTableOps(String location, Configuration conf) {
    return new HadoopTableOperations(new Path(location), conf);
  }
}
