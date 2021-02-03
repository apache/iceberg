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
  public void testLoadCatalog() throws IOException {
    conf.set(InputFormatConfig.CATALOG, Catalogs.LOCATION);
    Assert.assertFalse(Catalogs.loadCatalog(conf, null).isPresent());

    String nonExistentCatalogType = "fooType";

    conf.set(InputFormatConfig.CATALOG, nonExistentCatalogType);
    AssertHelpers.assertThrows(
            "should complain about catalog not supported", UnsupportedOperationException.class,
            "Unknown catalog type", () -> Catalogs.loadCatalog(conf, null));

    conf.set(InputFormatConfig.CATALOG, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION, "/tmp/mylocation");
    Optional<Catalog> hadoopCatalog = Catalogs.loadCatalog(conf, null);

    Assert.assertTrue(hadoopCatalog.isPresent());
    Assert.assertTrue(hadoopCatalog.get() instanceof HadoopCatalog);

    conf.set(InputFormatConfig.CATALOG, CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    Optional<Catalog> hiveCatalog = Catalogs.loadCatalog(conf, null);

    Assert.assertTrue(hiveCatalog.isPresent());
    Assert.assertTrue(hiveCatalog.get() instanceof HiveCatalog);

    conf.set(InputFormatConfig.CATALOG, Catalogs.LOCATION);
    Assert.assertFalse(Catalogs.loadCatalog(conf, null).isPresent());

    // arbitrary catalog name with non existent catalog type
    String catalogName = "barCatalog";
    conf.unset(InputFormatConfig.CATALOG);
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName), nonExistentCatalogType);
    AssertHelpers.assertThrows(
            "should complain about catalog not supported", UnsupportedOperationException.class,
            "Unknown catalog type:", () -> Catalogs.loadCatalog(conf, catalogName));

    // arbitrary catalog name with hadoop catalog type and default warehouse location
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName),
            CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    hadoopCatalog = Catalogs.loadCatalog(conf, catalogName);

    Assert.assertTrue(hadoopCatalog.isPresent());
    Assert.assertTrue(hadoopCatalog.get() instanceof HadoopCatalog);

    // arbitrary catalog name with hadoop catalog type and provided warehouse location
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName),
            CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(String.format(InputFormatConfig.CATALOG_WAREHOUSE_TEMPLATE, catalogName), "/tmp/mylocation");
    hadoopCatalog = Catalogs.loadCatalog(conf, catalogName);

    Assert.assertTrue(hadoopCatalog.isPresent());
    Assert.assertTrue(hadoopCatalog.get() instanceof HadoopCatalog);
    Assert.assertEquals("HadoopCatalog{name=barCatalog, location=/tmp/mylocation}", hadoopCatalog.get().toString());

    // arbitrary catalog name with hive catalog type
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName),
            CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    hiveCatalog = Catalogs.loadCatalog(conf, catalogName);

    Assert.assertTrue(hiveCatalog.isPresent());
    Assert.assertTrue(hiveCatalog.get() instanceof HiveCatalog);

    // arbitrary catalog name with custom catalog type without specific classloader
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName), "custom");
    AssertHelpers.assertThrows(
            "should complain about catalog not supported", UnsupportedOperationException.class,
            "Unknown catalog type:", () -> Catalogs.loadCatalog(conf, catalogName));

    // arbitrary catalog name with custom catalog type and provided classloader
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName), "custom");
    conf.set(String.format(InputFormatConfig.CATALOG_CLASS_TEMPLATE, catalogName), CustomHadoopCatalog.class.getName());
    Optional<Catalog> customHadoopCatalog = Catalogs.loadCatalog(conf, catalogName);

    Assert.assertTrue(customHadoopCatalog.isPresent());
    Assert.assertTrue(customHadoopCatalog.get() instanceof CustomHadoopCatalog);

    // arbitrary catalog name with location catalog type
    conf.unset(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName));
    Assert.assertFalse(Catalogs.loadCatalog(conf, catalogName).isPresent());

    // default catalog configuration
    conf.unset(InputFormatConfig.CATALOG);
    hiveCatalog = Catalogs.loadCatalog(conf, null);

    Assert.assertTrue(hiveCatalog.isPresent());
    Assert.assertTrue(hiveCatalog.get() instanceof HiveCatalog);
  }

  public static class CustomHadoopCatalog extends HadoopCatalog {

    public CustomHadoopCatalog() {

    }

    public CustomHadoopCatalog(Configuration conf, String warehouseLocation) {
      super(conf, warehouseLocation);
    }

  }

  private void setCustomCatalogProperties(String catalogName, String warehouseLocation) {
    conf.set(String.format(InputFormatConfig.CATALOG_WAREHOUSE_TEMPLATE, catalogName), warehouseLocation);
    conf.set(String.format(InputFormatConfig.CATALOG_CLASS_TEMPLATE, catalogName), CustomHadoopCatalog.class.getName());
    conf.set(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName), "custom");
    conf.set(InputFormatConfig.CATALOG_NAME, catalogName);
  }
}
