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
package org.apache.iceberg.mr;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCatalogs {

  private static final Schema SCHEMA = new Schema(required(1, "foo", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("foo").build();

  private Configuration conf;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() {
    conf = new Configuration();
  }

  @Test
  public void testLoadTableFromLocation() throws IOException {
    conf.set(CatalogUtil.ICEBERG_CATALOG_TYPE, Catalogs.LOCATION);

    Assertions.assertThatThrownBy(() -> Catalogs.loadTable(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table location not set");

    HadoopTables tables = new HadoopTables();
    Table hadoopTable = tables.create(SCHEMA, temp.newFolder("hadoop_tables").toString());

    conf.set(InputFormatConfig.TABLE_LOCATION, hadoopTable.location());

    Assert.assertEquals(hadoopTable.location(), Catalogs.loadTable(conf).location());
  }

  @Test
  public void testLoadTableFromCatalog() throws IOException {
    String defaultCatalogName = "default";
    String warehouseLocation = temp.newFolder("hadoop", "warehouse").toString();
    setCustomCatalogProperties(defaultCatalogName, warehouseLocation);

    Assertions.assertThatThrownBy(() -> Catalogs.loadTable(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table identifier not set");

    HadoopCatalog catalog = new CustomHadoopCatalog(conf, warehouseLocation);
    Table hadoopCatalogTable = catalog.createTable(TableIdentifier.of("table"), SCHEMA);

    conf.set(InputFormatConfig.TABLE_IDENTIFIER, "table");

    Assert.assertEquals(hadoopCatalogTable.location(), Catalogs.loadTable(conf).location());
  }

  @Test
  public void testCreateDropTableToLocation() throws IOException {
    Properties missingSchema = new Properties();
    missingSchema.put("location", temp.newFolder("hadoop_tables").toString());

    Assertions.assertThatThrownBy(() -> Catalogs.createTable(conf, missingSchema))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Table schema not set");

    conf.set(CatalogUtil.ICEBERG_CATALOG_TYPE, Catalogs.LOCATION);
    Properties missingLocation = new Properties();
    missingLocation.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(SCHEMA));

    Assertions.assertThatThrownBy(() -> Catalogs.createTable(conf, missingLocation))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Table location not set");

    Properties properties = new Properties();
    properties.put("location", temp.getRoot() + "/hadoop_tables");
    properties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(SCHEMA));
    properties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(SPEC));
    properties.put("dummy", "test");

    Catalogs.createTable(conf, properties);

    HadoopTables tables = new HadoopTables();
    Table table = tables.load(properties.getProperty("location"));

    Assert.assertEquals(properties.getProperty("location"), table.location());
    Assert.assertEquals(SchemaParser.toJson(SCHEMA), SchemaParser.toJson(table.schema()));
    Assert.assertEquals(PartitionSpecParser.toJson(SPEC), PartitionSpecParser.toJson(table.spec()));
    assertThat(table.properties()).containsEntry("dummy", "test");

    Assertions.assertThatThrownBy(() -> Catalogs.dropTable(conf, new Properties()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Table location not set");

    Properties dropProperties = new Properties();
    dropProperties.put("location", temp.getRoot() + "/hadoop_tables");
    Catalogs.dropTable(conf, dropProperties);

    Assertions.assertThatThrownBy(() -> Catalogs.loadTable(conf, dropProperties))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist at location: " + properties.getProperty("location"));
  }

  @Test
  public void testCreateDropTableToCatalog() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("test", "table");
    String defaultCatalogName = "default";
    String warehouseLocation = temp.newFolder("hadoop", "warehouse").toString();

    setCustomCatalogProperties(defaultCatalogName, warehouseLocation);

    Properties missingSchema = new Properties();
    missingSchema.put("name", identifier.toString());
    missingSchema.put(InputFormatConfig.CATALOG_NAME, defaultCatalogName);

    Assertions.assertThatThrownBy(() -> Catalogs.createTable(conf, missingSchema))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Table schema not set");

    Properties missingIdentifier = new Properties();
    missingIdentifier.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(SCHEMA));
    missingIdentifier.put(InputFormatConfig.CATALOG_NAME, defaultCatalogName);
    Assertions.assertThatThrownBy(() -> Catalogs.createTable(conf, missingIdentifier))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Table identifier not set");

    Properties properties = new Properties();
    properties.put("name", identifier.toString());
    properties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(SCHEMA));
    properties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(SPEC));
    properties.put("dummy", "test");
    properties.put(InputFormatConfig.CATALOG_NAME, defaultCatalogName);

    Catalogs.createTable(conf, properties);

    HadoopCatalog catalog = new CustomHadoopCatalog(conf, warehouseLocation);
    Table table = catalog.loadTable(identifier);

    Assert.assertEquals(SchemaParser.toJson(SCHEMA), SchemaParser.toJson(table.schema()));
    Assert.assertEquals(PartitionSpecParser.toJson(SPEC), PartitionSpecParser.toJson(table.spec()));
    assertThat(table.properties()).containsEntry("dummy", "test");

    Assertions.assertThatThrownBy(() -> Catalogs.dropTable(conf, new Properties()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Table identifier not set");

    Properties dropProperties = new Properties();
    dropProperties.put("name", identifier.toString());
    dropProperties.put(InputFormatConfig.CATALOG_NAME, defaultCatalogName);
    Catalogs.dropTable(conf, dropProperties);

    Assertions.assertThatThrownBy(() -> Catalogs.loadTable(conf, dropProperties))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: test.table");
  }

  @Test
  public void testLoadCatalogDefault() {
    String catalogName = "barCatalog";
    Optional<Catalog> defaultCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(defaultCatalog.isPresent());
    Assertions.assertThat(defaultCatalog.get()).isInstanceOf(HiveCatalog.class);
    Properties properties = new Properties();
    properties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    Assert.assertTrue(Catalogs.hiveCatalog(conf, properties));
  }

  @Test
  public void testLoadCatalogHive() {
    String catalogName = "barCatalog";
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    Optional<Catalog> hiveCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(hiveCatalog.isPresent());
    Assertions.assertThat(hiveCatalog.get()).isInstanceOf(HiveCatalog.class);
    Properties properties = new Properties();
    properties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    Assert.assertTrue(Catalogs.hiveCatalog(conf, properties));
  }

  @Test
  public void testLoadCatalogHadoop() {
    String catalogName = "barCatalog";
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(
            catalogName, CatalogProperties.WAREHOUSE_LOCATION),
        "/tmp/mylocation");
    Optional<Catalog> hadoopCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(hadoopCatalog.isPresent());
    Assertions.assertThat(hadoopCatalog.get()).isInstanceOf(HadoopCatalog.class);
    Assert.assertEquals(
        "HadoopCatalog{name=barCatalog, location=/tmp/mylocation}", hadoopCatalog.get().toString());
    Properties properties = new Properties();
    properties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    Assert.assertFalse(Catalogs.hiveCatalog(conf, properties));
  }

  @Test
  public void testLoadCatalogCustom() {
    String catalogName = "barCatalog";
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.CATALOG_IMPL),
        CustomHadoopCatalog.class.getName());
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(
            catalogName, CatalogProperties.WAREHOUSE_LOCATION),
        "/tmp/mylocation");
    Optional<Catalog> customHadoopCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(customHadoopCatalog.isPresent());
    Assertions.assertThat(customHadoopCatalog.get()).isInstanceOf(CustomHadoopCatalog.class);
    Properties properties = new Properties();
    properties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    Assert.assertFalse(Catalogs.hiveCatalog(conf, properties));
  }

  @Test
  public void testLoadCatalogLocation() {
    Assert.assertFalse(Catalogs.loadCatalog(conf, Catalogs.ICEBERG_HADOOP_TABLE_NAME).isPresent());
  }

  @Test
  public void testLoadCatalogUnknown() {
    String catalogName = "barCatalog";
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        "fooType");

    Assertions.assertThatThrownBy(() -> Catalogs.loadCatalog(conf, catalogName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unknown catalog type: fooType");
  }

  public static class CustomHadoopCatalog extends HadoopCatalog {

    public CustomHadoopCatalog() {}

    public CustomHadoopCatalog(Configuration conf, String warehouseLocation) {
      super(conf, warehouseLocation);
    }
  }

  private void setCustomCatalogProperties(String catalogName, String warehouseLocation) {
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(
            catalogName, CatalogProperties.WAREHOUSE_LOCATION),
        warehouseLocation);
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.CATALOG_IMPL),
        CustomHadoopCatalog.class.getName());
    conf.set(InputFormatConfig.CATALOG_NAME, catalogName);
  }
}
