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
package com.netflix.iceberg.hive;

import com.google.common.base.Splitter;
import com.netflix.iceberg.BaseMetastoreTableOperations;
import com.netflix.iceberg.BaseMetastoreTables;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.THRIFT_URIS;

public class HiveTables extends BaseMetastoreTables {
  private static final Splitter DOT = Splitter.on('.').limit(2);
  private Configuration conf;

  public HiveTables(Configuration conf) {
    super(conf);
    this.conf = conf;
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
    return new HiveTableOperations(conf, getClient(), database, table);
  }

  private ThriftHiveMetastore.Client getClient() {
    final URI metastoreUri = URI.create(MetastoreConf.getAsString(conf, THRIFT_URIS));
    final int socketTimeOut = (int) MetastoreConf.getTimeVar(conf, CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
    TTransport transport = new TSocket(metastoreUri.getHost(), metastoreUri.getPort(), socketTimeOut);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new RuntimeException("failed to open socket for " + metastoreUri + " with timeoutMillis " + socketTimeOut);
    }
    return new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
  }
}
