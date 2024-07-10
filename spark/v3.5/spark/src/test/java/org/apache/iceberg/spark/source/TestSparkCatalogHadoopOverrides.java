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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.KryoHelpers;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkCatalogHadoopOverrides extends CatalogTestBase {

  private static final String CONFIG_TO_OVERRIDE = "fs.s3a.buffer.dir";
  // prepend "hadoop." so that the test base formats SQLConf correctly
  //   as `spark.sql.catalogs.<catalogName>.hadoop.<configToOverride>
  private static final String HADOOP_PREFIXED_CONFIG_TO_OVERRIDE = "hadoop." + CONFIG_TO_OVERRIDE;
  private static final String CONFIG_OVERRIDE_VALUE = "/tmp-overridden";

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
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
            HADOOP_PREFIXED_CONFIG_TO_OVERRIDE,
            CONFIG_OVERRIDE_VALUE)
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop", HADOOP_PREFIXED_CONFIG_TO_OVERRIDE, CONFIG_OVERRIDE_VALUE)
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            HADOOP_PREFIXED_CONFIG_TO_OVERRIDE,
            CONFIG_OVERRIDE_VALUE)
      }
    };
  }

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE IF NOT EXISTS %s (id bigint) USING iceberg", tableName(tableIdent.name()));
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName(tableIdent.name()));
  }

  @TestTemplate
  public void testTableFromCatalogHasOverrides() throws Exception {
    Table table = getIcebergTableFromSparkCatalog();
    Configuration conf = ((Configurable) table.io()).getConf();
    String actualCatalogOverride = conf.get(CONFIG_TO_OVERRIDE, "/whammies");
    assertThat(actualCatalogOverride)
        .as(
            "Iceberg tables from spark should have the overridden hadoop configurations from the spark config")
        .isEqualTo(CONFIG_OVERRIDE_VALUE);
  }

  @TestTemplate
  public void ensureRoundTripSerializedTableRetainsHadoopConfig() throws Exception {
    Table table = getIcebergTableFromSparkCatalog();
    Configuration originalConf = ((Configurable) table.io()).getConf();
    String actualCatalogOverride = originalConf.get(CONFIG_TO_OVERRIDE, "/whammies");
    assertThat(actualCatalogOverride)
        .as(
            "Iceberg tables from spark should have the overridden hadoop configurations from the spark config")
        .isEqualTo(CONFIG_OVERRIDE_VALUE);

    // Now convert to SerializableTable and ensure overridden property is still present.
    Table serializableTable = SerializableTableWithSize.copyOf(table);
    Table kryoSerializedTable =
        KryoHelpers.roundTripSerialize(SerializableTableWithSize.copyOf(table));
    Configuration configFromKryoSerde = ((Configurable) kryoSerializedTable.io()).getConf();
    String kryoSerializedCatalogOverride = configFromKryoSerde.get(CONFIG_TO_OVERRIDE, "/whammies");
    assertThat(kryoSerializedCatalogOverride)
        .as(
            "Tables serialized with Kryo serialization should retain overridden hadoop configuration properties")
        .isEqualTo(CONFIG_OVERRIDE_VALUE);

    // Do the same for Java based serde
    Table javaSerializedTable = TestHelpers.roundTripSerialize(serializableTable);
    Configuration configFromJavaSerde = ((Configurable) javaSerializedTable.io()).getConf();
    String javaSerializedCatalogOverride = configFromJavaSerde.get(CONFIG_TO_OVERRIDE, "/whammies");
    assertThat(javaSerializedCatalogOverride)
        .as(
            "Tables serialized with Java serialization should retain overridden hadoop configuration properties")
        .isEqualTo(CONFIG_OVERRIDE_VALUE);
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
