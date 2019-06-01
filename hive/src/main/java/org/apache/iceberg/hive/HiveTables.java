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

package org.apache.iceberg.hive;

import com.google.common.base.Splitter;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseMetastoreTables;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

public class HiveTables extends BaseMetastoreTables implements Closeable {
  private static final Splitter DOT = Splitter.on('.').limit(2);
  private final HiveClientPool clients;

  public HiveTables(Configuration conf) {
    super(conf);
    this.clients = new HiveClientPool(2, conf);
  }

  @Override
  public Table create(Schema schema, String tableIdentifier) {
    return create(schema, PartitionSpec.unpartitioned(), tableIdentifier);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, Map<String, String> properties, String tableIdentifier) {
    List<String> parts = DOT.splitToList(tableIdentifier);
    if (parts.size() == 2) {
      return create(schema, spec, properties, parts.get(0), parts.get(1));
    }
    throw new UnsupportedOperationException("Could not parse table identifier: " + tableIdentifier);
  }

  @Override
  public Table load(String tableIdentifier) {
    List<String> parts = DOT.splitToList(tableIdentifier);
    if (parts.size() == 2) {
      return load(parts.get(0), parts.get(1));
    }
    throw new UnsupportedOperationException("Could not parse table identifier: " + tableIdentifier);
  }

  @Override
  public BaseMetastoreTableOperations newTableOps(Configuration conf, String database, String table) {
    return new HiveTableOperations(conf, clients, database, table);
  }

  @Override
  public void close() {
    clients.close();
  }
}
