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

package org.apache.iceberg.mr.hive;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestCatalogs;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

// Helper class for setting up and testing various catalog implementations
abstract class TestTables {

  private final Tables tables;
  protected final TemporaryFolder temp;

  protected TestTables(Tables tables, TemporaryFolder temp) {
    this.tables = tables;
    this.temp = temp;
  }

  protected TestTables(Catalog catalog, TemporaryFolder temp) {
    this(new CatalogToTables(catalog), temp);
  }

  public Map<String, String> properties() {
    return Collections.emptyMap();
  }

  // For HadoopTables this method will return a temporary location
  public String identifier(String tableIdentifier) {
    return tableIdentifier;
  }

  public Tables tables() {
    return tables;
  }

  /**
   * The location string needed to be provided for CREATE TABLE ... commands,
   * like "LOCATION 'file:///tmp/warehouse/default/tablename'. Empty ("") if LOCATION is not needed.
   * @param identifier The table identifier
   * @return The location string for create table operation
   */
  public abstract String locationForCreateTableSQL(TableIdentifier identifier);

  /**
   * If the {@link Catalogs#LOCATION} is needed for {@link Catalogs#loadTable(Configuration, Properties)} then this
   * method should provide the location string. It should return <code>null</code> if the location is not needed.
   * @param identifier The table identifier
   * @return The location string for loadTable operation
   */
  public abstract String loadLocation(TableIdentifier identifier);

  private static class CatalogToTables implements Tables {

    private final Catalog catalog;

    private CatalogToTables(Catalog catalog) {
      this.catalog = catalog;
    }

    @Override
    public Table create(Schema schema, PartitionSpec spec, SortOrder sortOrder,
                        Map<String, String> properties, String tableIdentifier) {
      TableIdentifier tableIdent = TableIdentifier.parse(tableIdentifier);
      return catalog.buildTable(tableIdent, schema)
          .withPartitionSpec(spec)
          .withSortOrder(sortOrder)
          .withProperties(properties)
          .create();
    }

    @Override
    public Table load(String tableIdentifier) {
      return catalog.loadTable(TableIdentifier.parse(tableIdentifier));
    }
  }

  static class CustomCatalogTestTables extends TestTables {

    private final String warehouseLocation;

    CustomCatalogTestTables(Configuration conf, TemporaryFolder temp) throws IOException {
      this(conf, temp, (MetastoreUtil.hive3PresentOnClasspath() ? "file:" : "") +
          temp.newFolder("custom", "warehouse").toString());
    }

    CustomCatalogTestTables(Configuration conf, TemporaryFolder temp, String warehouseLocation) {
      super(new TestCatalogs.CustomHadoopCatalog(conf, warehouseLocation), temp);
      this.warehouseLocation = warehouseLocation;
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(
              InputFormatConfig.CATALOG_LOADER_CLASS, TestCatalogs.CustomHadoopCatalogLoader.class.getName(),
              TestCatalogs.CustomHadoopCatalog.WAREHOUSE_LOCATION, warehouseLocation
      );
    }

    @Override
    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "LOCATION '" + warehouseLocation + TestTables.tablePath(identifier) + "' ";
    }

    @Override
    public String loadLocation(TableIdentifier identifier) {
      return warehouseLocation + TestTables.tablePath(identifier);
    }
  }

  static class HadoopCatalogTestTables extends TestTables {

    private final String warehouseLocation;

    HadoopCatalogTestTables(Configuration conf, TemporaryFolder temp) throws IOException {
      this(conf, temp, (MetastoreUtil.hive3PresentOnClasspath() ? "file:" : "") +
          temp.newFolder("hadoop", "warehouse").toString());
    }

    HadoopCatalogTestTables(Configuration conf, TemporaryFolder temp, String warehouseLocation) {
      super(new HadoopCatalog(conf, warehouseLocation), temp);
      this.warehouseLocation = warehouseLocation;
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(
              InputFormatConfig.CATALOG, "hadoop",
              InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION, warehouseLocation
      );
    }

    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "LOCATION '" + warehouseLocation + TestTables.tablePath(identifier) + "' ";
    }

    public String loadLocation(TableIdentifier identifier) {
      return null;
    }
  }

  static class HadoopTestTables extends TestTables {

    HadoopTestTables(Configuration conf, TemporaryFolder temp) {
      super(new HadoopTables(conf), temp);
    }

    @Override
    public String identifier(String tableIdentifier) {
      final File location;

      try {
        TableIdentifier identifier = TableIdentifier.parse(tableIdentifier);
        location = temp.newFolder(ObjectArrays.concat(identifier.namespace().levels(), identifier.name()));
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }

      Assert.assertTrue(location.delete());
      return location.toString();
    }

    @Override
    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "LOCATION '" + temp.getRoot().getPath() + tablePath(identifier) + "' ";
    }

    @Override
    public String loadLocation(TableIdentifier identifier) {
      return temp.getRoot().getPath() + TestTables.tablePath(identifier);
    }
  }

  static class HiveTestTables extends TestTables {

    HiveTestTables(Configuration conf, TemporaryFolder temp) {
      super(HiveCatalogs.loadCatalog(conf), temp);
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(InputFormatConfig.CATALOG, "hive");
    }

    @Override
    public String locationForCreateTableSQL(TableIdentifier identifier) {
      return "";
    }

    public String loadLocation(TableIdentifier identifier) {
      return null;
    }
  }

  private static String tablePath(TableIdentifier identifier) {
    return "/" + Joiner.on("/").join(identifier.namespace().levels()) + "/" + identifier.name();
  }
}
