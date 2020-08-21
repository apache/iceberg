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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
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

  private Configuration conf;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() {
    conf = new Configuration();
  }

  @Test
  public void testLoadTableFromLocation() throws IOException {
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
    conf.set("warehouse.location", temp.newFolder("hadoop", "warehouse").toString());
    conf.setClass(InputFormatConfig.CATALOG_LOADER_CLASS, CustomHadoopCatalogLoader.class, CatalogLoader.class);

    AssertHelpers.assertThrows(
            "Should complain about table identifier not set", IllegalArgumentException.class,
            "identifier not set", () -> Catalogs.loadTable(conf));

    HadoopCatalog catalog = new CustomHadoopCatalog(conf);
    Table hadoopCatalogTable = catalog.createTable(TableIdentifier.of("table"), SCHEMA);

    conf.set(InputFormatConfig.TABLE_IDENTIFIER, "table");

    Assert.assertEquals(hadoopCatalogTable.location(), Catalogs.loadTable(conf).location());
  }

  @Test
  public void testLoadCatalog() throws IOException {
    Assert.assertFalse(Catalogs.loadCatalog(conf).isPresent());

    conf.set(InputFormatConfig.CATALOG, "foo");
    AssertHelpers.assertThrows(
            "Should complain about catalog not supported", NoSuchNamespaceException.class,
            "is not supported", () -> Catalogs.loadCatalog(conf));

    conf.set(InputFormatConfig.CATALOG, "hadoop");
    Optional<Catalog> hadoopCatalog = Catalogs.loadCatalog(conf);

    Assert.assertTrue(hadoopCatalog.isPresent());
    Assert.assertTrue(hadoopCatalog.get() instanceof HadoopCatalog);

    conf.set(InputFormatConfig.CATALOG, "hive");
    Optional<Catalog> hiveCatalog = Catalogs.loadCatalog(conf);

    Assert.assertTrue(hiveCatalog.isPresent());
    Assert.assertTrue(hiveCatalog.get() instanceof HiveCatalog);

    conf.set("warehouse.location", temp.newFolder("hadoop", "warehouse").toString());
    conf.setClass(InputFormatConfig.CATALOG_LOADER_CLASS, CustomHadoopCatalogLoader.class, CatalogLoader.class);
    Optional<Catalog> customHadoopCatalog = Catalogs.loadCatalog(conf);

    Assert.assertTrue(customHadoopCatalog.isPresent());
    Assert.assertTrue(customHadoopCatalog.get() instanceof CustomHadoopCatalog);
  }

  public static class CustomHadoopCatalog extends HadoopCatalog {

    public static final String WAREHOUSE_LOCATION = "warehouse.location";

    public CustomHadoopCatalog(Configuration conf, String warehouseLocation) {
      super(conf, warehouseLocation);
    }

    public CustomHadoopCatalog(Configuration conf) {
      this(conf, conf.get(WAREHOUSE_LOCATION));
    }
  }

  public static class CustomHadoopCatalogLoader implements CatalogLoader {
    @Override
    public Catalog load(Configuration conf) {
      return new CustomHadoopCatalog(conf);
    }
  }
}
