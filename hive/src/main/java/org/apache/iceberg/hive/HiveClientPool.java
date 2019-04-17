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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

class HiveClientPool extends ClientPool<HiveMetaStoreClient, TException> {
  private final HiveConf hiveConf;

  HiveClientPool(Configuration conf) {
    this(conf.getInt("iceberg.hive.client-pool-size", 5), conf);
  }

  HiveClientPool(int poolSize, Configuration conf) {
    super(poolSize, client -> {
      try {
        client.reconnect();
      } catch (MetaException e) {
        throw new RuntimeMetaException(e, "Failed to reconnect to Hive Metastore");
      }
    }, TException.class, HiveMetaStoreClient::close);
    this.hiveConf = new HiveConf(conf, HiveClientPool.class);
  }

  @Override
  protected HiveMetaStoreClient newClient()  {
    try {
      return new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
    }
  }
}
