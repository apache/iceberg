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

package org.apache.iceberg.flink;

import java.util.concurrent.ConcurrentMap;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class FlinkTestBase extends AbstractTestBase {

  private static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static HiveCatalog catalog = null;
  protected static ConcurrentMap<String, Catalog> flinkCatalogs;

  @BeforeClass
  public static void startMetastoreAndSpark() {
    FlinkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    FlinkTestBase.hiveConf = metastore.hiveConf();
    FlinkTestBase.catalog = new HiveCatalog(metastore.hiveConf());
    flinkCatalogs = Maps.newConcurrentMap();
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    metastore.stop();
    catalog.close();
    FlinkTestBase.catalog = null;
    flinkCatalogs.values().forEach(Catalog::close);
    flinkCatalogs.clear();
  }
}
