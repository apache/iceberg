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

import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HiveMetastoreExtension implements BeforeEachCallback, AfterEachCallback {
  private HiveMetaStoreClient metastoreClient;
  private TestHiveMetastore metastore;
  private final Map<String, String> hiveConfOverride;
  private final String databaseName;

  public HiveMetastoreExtension(String databaseName, Map<String, String> hiveConfOverride) {
    this.databaseName = databaseName;
    this.hiveConfOverride = hiveConfOverride;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    metastore = new TestHiveMetastore();
    HiveConf hiveConfWithOverrides = new HiveConf(TestHiveMetastore.class);
    if (hiveConfOverride != null) {
      for (Map.Entry<String, String> kv : hiveConfOverride.entrySet()) {
        hiveConfWithOverrides.set(kv.getKey(), kv.getValue());
      }
    }

    metastore.start(hiveConfWithOverrides);
    metastoreClient = new HiveMetaStoreClient(hiveConfWithOverrides);

    String dbPath = metastore.getDatabasePath(databaseName);
    Database db = new Database(databaseName, "description", dbPath, Maps.newHashMap());
    metastoreClient.createDatabase(db);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    if (null != metastoreClient) {
      metastoreClient.close();
    }

    if (null != metastore) {
      metastore.stop();
    }

    metastoreClient = null;
    metastore = null;
  }

  public HiveMetaStoreClient metastoreClient() {
    return metastoreClient;
  }

  public HiveConf hiveConf() {
    return metastore.hiveConf();
  }
}
