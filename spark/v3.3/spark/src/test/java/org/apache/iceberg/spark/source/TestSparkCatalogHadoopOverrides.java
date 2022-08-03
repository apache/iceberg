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

import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.KryoHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestSparkCatalogHadoopOverrides extends SparkCatalogTestBase {

  private static final String configToOverride = "fs.s3a.buffer.dir";
  // prepend "hadoop." so that the test base formats SQLConf correctly
  //   as `spark.sql.catalogs.<catalogName>.hadoop.<configToOverride>
  private static final String hadoopPrefixedConfigToOverride = "hadoop." + configToOverride;
  private static final String configOverrideValue = "/tmp-overridden";

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            hadoopPrefixedConfigToOverride,
            configOverrideValue)
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop", hadoopPrefixedConfigToOverride, configOverrideValue)
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            hadoopPrefixedConfigToOverride,
            configOverrideValue)
      }
    };
  }

  public TestSparkCatalogHadoopOverrides(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTable() {
    sql("CREATE TABLE IF NOT EXISTS %s (id bigint) USING iceberg", tableName(tableIdent.name()));
  }

  @After
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName(tableIdent.name()));
  }

  @Test
  public void testTableFromCatalogHasOverrides() throws Exception {
    Table table = getIcebergTableFromSparkCatalog();
    Configuration conf = ((Configurable) table.io()).getConf();
    String actualCatalogOverride = conf.get(configToOverride, "/whammies");
    Assert.assertEquals(
        "Iceberg tables from spark should have the overridden hadoop configurations from the spark config",
        configOverrideValue,
        actualCatalogOverride);
  }

  @Test
  public void ensureRoundTripSerializedTableRetainsHadoopConfig() throws Exception {
    Table table = getIcebergTableFromSparkCatalog();
    Configuration originalConf = ((Configurable) table.io()).getConf();
    String actualCatalogOverride = originalConf.get(configToOverride, "/whammies");
    Assert.assertEquals(
        "Iceberg tables from spark should have the overridden hadoop configurations from the spark config",
        configOverrideValue,
        actualCatalogOverride);

    // Now convert to SerializableTable and ensure overridden property is still present.
    Table serializableTable = SerializableTableWithSize.copyOf(table);
    Table kryoSerializedTable =
        KryoHelpers.roundTripSerialize(SerializableTableWithSize.copyOf(table));
    Configuration configFromKryoSerde = ((Configurable) kryoSerializedTable.io()).getConf();
    String kryoSerializedCatalogOverride = configFromKryoSerde.get(configToOverride, "/whammies");
    Assert.assertEquals(
        "Tables serialized with Kryo serialization should retain overridden hadoop configuration properties",
        configOverrideValue,
        kryoSerializedCatalogOverride);

    // Do the same for Java based serde
    Table javaSerializedTable = TestHelpers.roundTripSerialize(serializableTable);
    Configuration configFromJavaSerde = ((Configurable) javaSerializedTable.io()).getConf();
    String javaSerializedCatalogOverride = configFromJavaSerde.get(configToOverride, "/whammies");
    Assert.assertEquals(
        "Tables serialized with Java serialization should retain overridden hadoop configuration properties",
        configOverrideValue,
        javaSerializedCatalogOverride);
  }

  @SuppressWarnings("ThrowSpecificity")
  private Table getIcebergTableFromSparkCatalog() throws Exception {
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    SparkTable sparkTable = (SparkTable) catalog.loadTable(identifier);
    return sparkTable.table();
  }
}
