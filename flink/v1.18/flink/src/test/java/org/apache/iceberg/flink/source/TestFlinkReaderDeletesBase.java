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
package org.apache.iceberg.flink.source;

import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DeleteReadTests;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestFlinkReaderDeletesBase extends DeleteReadTests {

  protected static String databaseName = "default";

  protected static HiveConf hiveConf = null;
  protected static HiveCatalog catalog = null;
  private static TestHiveMetastore metastore = null;

  @BeforeAll
  public static void startMetastore() {
    metastore = new TestHiveMetastore();
    metastore.start();
    hiveConf = metastore.hiveConf();
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @AfterAll
  public static void stopMetastore() throws Exception {
    metastore.stop();
    catalog = null;
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    Table table = catalog.createTable(TableIdentifier.of(databaseName, name), schema, spec, props);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of(databaseName, name));
  }

  @Override
  protected boolean expectPruned() {
    return false;
  }
}
