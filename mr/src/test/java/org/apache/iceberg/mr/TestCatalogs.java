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

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
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

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestCatalogs {

  private static final Schema SCHEMA = new Schema(required(1, "foo", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("foo").build();

  private Configuration conf;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() {
    conf = new Configuration();
  }

  @Test
  public void testLoadTableFromLocation() throws IOException {
    conf.set(InputFormatConfig.CATALOG, Catalogs.LOCATION);
    AssertHelpers.assertThrows(
            "Should complain about table location not set", IllegalArgumentException.class,
            "location not set", () -> Catalogs.loadTable(conf));

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

    AssertHelpers.assertThrows(
            "Should complain about table identifier not set", IllegalArgumentException.class,
            "identifier not set", () -> Catalogs.loadTable(conf));

    HadoopCatalog catalog = new CustomHadoopCatalog(conf, warehouseLocation);
    Table hadoopCatalogTable = catalog.createTable(TableIdentifier.of("table"), SCHEMA);

    conf.set(InputFormatConfig.TABLE_IDENTIFIER, "table");

    Assert.assertEquals(hadoopCatalogTable.location(), Catalogs.loadTable(conf).location());
  }

  @Test
  public void testCreateDropTableToLocation() throws IOException {
    Properties missingSchema = new Properties();
    missingSchema.put("location", temp.newFolder("hadoop_tables").toString());
    AssertHelpers.assertThrows(
        "Should complain about table schema not set", NullPointerException.class,
        "schema not set", () -> Catalogs.createTable(conf, missingSchema));

    conf.set(InputFormatConfig.CATALOG, Catalogs.LOCATION);
    Properties missingLocation = new Properties();
    missingLocation.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(SCHEMA));
    AssertHelpers.assertThrows(
        "Should complain about table location not set", NullPointerException.class,
        "location not set", () -> Catalogs.createTable(conf, missingLocation));

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
    Assert.assertEquals(Collections.singletonMap("dummy", "test"), table.properties());

    AssertHelpers.assertThrows(
        "Should complain about table location not set", NullPointerException.class,
        "location not set", () -> Catalogs.dropTable(conf, new Properties()));

    Properties dropProperties = new Properties();
    dropProperties.put("location", temp.getRoot() + "/hadoop_tables");
    Catalogs.dropTable(conf, dropProperties);

    AssertHelpers.assertThrows(
        "Should complain about table not found", NoSuchTableException.class,
        "Table does not exist", () -> Catalogs.loadTable(conf, dropProperties));
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
    AssertHelpers.assertThrows(
        "Should complain about table schema not set", NullPointerException.class,
        "schema not set", () -> Catalogs.createTable(conf, missingSchema));

    Properties missingIdentifier = new Properties();
    missingIdentifier.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(SCHEMA));
    missingIdentifier.put(InputFormatConfig.CATALOG_NAME, defaultCatalogName);
    AssertHelpers.assertThrows(
        "Should complain about table identifier not set", NullPointerException.class,
        "identifier not set", () -> Catalogs.createTable(conf, missingIdentifier));

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
    Assert.assertEquals(Collections.singletonMap("dummy", "test"), table.properties());

    AssertHelpers.assertThrows(
        "Should complain about table identifier not set", NullPointerException.class,
        "identifier not set", () -> Catalogs.dropTable(conf, new Properties()));

    Properties dropProperties = new Properties();
    dropProperties.put("name", identifier.toString());
    dropProperties.put(InputFormatConfig.CATALOG_NAME, defaultCatalogName);
    Catalogs.dropTable(conf, dropProperties);

    AssertHelpers.assertThrows(
        "Should complain about table not found", NoSuchTableException.class,
        "Table does not exist", () -> Catalogs.loadTable(conf, dropProperties));
  }

  @Test
  public void testLegacyLoadCatalogDefault() {
    Optional<Catalog> defaultCatalog = Catalogs.loadCatalog(conf, null);
    Assert.assertTrue(defaultCatalog.isPresent());
    Assertions.assertThat(defaultCatalog.get()).isInstanceOf(HiveCatalog.class);
  }

  @Test
  public void testLegacyLoadCatalogHive() {
    conf.set(InputFormatConfig.CATALOG, CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    Optional<Catalog> hiveCatalog = Catalogs.loadCatalog(conf, null);
    Assert.assertTrue(hiveCatalog.isPresent());
    Assertions.assertThat(hiveCatalog.get()).isInstanceOf(HiveCatalog.class);
  }

  @Test
  public void testLegacyLoadCatalogHadoop() {
    conf.set(InputFormatConfig.CATALOG, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION, "/tmp/mylocation");
    Optional<Catalog> hadoopCatalog = Catalogs.loadCatalog(conf, null);
    Assert.assertTrue(hadoopCatalog.isPresent());
    Assertions.assertThat(hadoopCatalog.get()).isInstanceOf(HadoopCatalog.class);
  }

  @Test
  public void testLegacyLoadCatalogCustom() {
    conf.set(InputFormatConfig.CATALOG_LOADER_CLASS, CustomHadoopCatalog.class.getName());
    conf.set(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION, "/tmp/mylocation");
    Optional<Catalog> customHadoopCatalog = Catalogs.loadCatalog(conf, null);
    Assert.assertTrue(customHadoopCatalog.isPresent());
    Assertions.assertThat(customHadoopCatalog.get()).isInstanceOf(CustomHadoopCatalog.class);
  }

  @Test
  public void testLegacyLoadCatalogLocation() {
    conf.set(InputFormatConfig.CATALOG, Catalogs.LOCATION);
    Assert.assertFalse(Catalogs.loadCatalog(conf, null).isPresent());
  }

  @Test
  public void testLegacyLoadCatalogUnknown() {
    conf.set(InputFormatConfig.CATALOG, "fooType");
    AssertHelpers.assertThrows(
            "should complain about catalog not supported", UnsupportedOperationException.class,
            "Unknown catalog type", () -> Catalogs.loadCatalog(conf, null));
  }

  @Test
  public void testLoadCatalogDefault() {
    Optional<Catalog> defaultCatalog = Catalogs.loadCatalog(conf, "barCatalog");
    Assert.assertTrue(defaultCatalog.isPresent());
    Assertions.assertThat(defaultCatalog.get()).isInstanceOf(HiveCatalog.class);
  }

  @Test
  public void testLoadCatalogHive() {
    String catalogName = "barCatalog";
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    Optional<Catalog> hiveCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(hiveCatalog.isPresent());
    Assertions.assertThat(hiveCatalog.get()).isInstanceOf(HiveCatalog.class);
  }

  @Test
  public void testLoadCatalogHadoop() {
    String catalogName = "barCatalog";
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.WAREHOUSE_LOCATION),
        "/tmp/mylocation");
    Optional<Catalog> hadoopCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(hadoopCatalog.isPresent());
    Assertions.assertThat(hadoopCatalog.get()).isInstanceOf(HadoopCatalog.class);
    Assert.assertEquals("HadoopCatalog{name=barCatalog, location=/tmp/mylocation}", hadoopCatalog.get().toString());
  }

  @Test
  public void testLoadCatalogHadoopWithLegacyWarehouseLocation() {
    String catalogName = "barCatalog";
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION, "/tmp/mylocation");
    Optional<Catalog> hadoopCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(hadoopCatalog.isPresent());
    Assertions.assertThat(hadoopCatalog.get()).isInstanceOf(HadoopCatalog.class);
    Assert.assertEquals("HadoopCatalog{name=barCatalog, location=/tmp/mylocation}", hadoopCatalog.get().toString());
  }

  @Test
  public void testLoadCatalogCustom() {
    String catalogName = "barCatalog";
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.CATALOG_IMPL),
        CustomHadoopCatalog.class.getName());
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.WAREHOUSE_LOCATION),
        "/tmp/mylocation");
    Optional<Catalog> customHadoopCatalog = Catalogs.loadCatalog(conf, catalogName);
    Assert.assertTrue(customHadoopCatalog.isPresent());
    Assertions.assertThat(customHadoopCatalog.get()).isInstanceOf(CustomHadoopCatalog.class);
  }

  @Test
  public void testLoadCatalogLocation() {
    Assert.assertFalse(Catalogs.loadCatalog(conf, Catalogs.ICEBERG_HADOOP_TABLE_NAME).isPresent());
  }

  @Test
  public void testLoadCatalogUnknown() {
    String catalogName = "barCatalog";
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE), "fooType");
    AssertHelpers.assertThrows(
        "should complain about catalog not supported", UnsupportedOperationException.class,
        "Unknown catalog type:", () -> Catalogs.loadCatalog(conf, catalogName));
  }

  public static class CustomHadoopCatalog extends HadoopCatalog {

    public CustomHadoopCatalog() {

    }

    public CustomHadoopCatalog(Configuration conf, String warehouseLocation) {
      super(conf, warehouseLocation);
    }

  }

  private void setCustomCatalogProperties(String catalogName, String warehouseLocation) {
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.WAREHOUSE_LOCATION),
        warehouseLocation);
    conf.set(InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogProperties.CATALOG_IMPL),
        CustomHadoopCatalog.class.getName());
    conf.set(InputFormatConfig.CATALOG_NAME, catalogName);
  }
}
