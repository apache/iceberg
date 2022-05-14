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

import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class FlinkDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
  static final String FACTORY_IDENTIFIER = "iceberg";

  private static final ConfigOption<String> CATALOG_NAME =
      ConfigOptions.key("catalog-name")
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog name");

  private static final ConfigOption<String> CATALOG_DATABASE =
      ConfigOptions.key("catalog-database")
          .stringType()
          .defaultValue(FlinkCatalog.DEFAULT_DATABASE)
          .withDescription("Database name managed in the iceberg catalog.");

  private static final ConfigOption<String> CATALOG_TABLE =
      ConfigOptions.key("catalog-table")
          .stringType()
          .noDefaultValue()
          .withDescription("Table name managed in the underlying iceberg catalog and database.");

  private final FlinkCatalog catalog;

  public FlinkDynamicTableFactory() {
    this.catalog = null;
  }

  public FlinkDynamicTableFactory(FlinkCatalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    validateOptions(context);

    ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
    CatalogTable catalogTable = context.getCatalogTable();

    TableLoader tableLoader = createTableLoader(catalogTable, objectPath, context.getClassLoader());
    TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());

    return new IcebergTableSource(tableLoader, tableSchema, catalogTable.getOptions(), context.getConfiguration());
  }


  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    validateOptions(context);

    ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
    CatalogTable catalogTable = context.getCatalogTable();

    TableLoader tableLoader = createTableLoader(catalogTable, objectPath, context.getClassLoader());
    TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());

    return new IcebergTableSink(tableLoader, tableSchema, context.getConfiguration());
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = Sets.newHashSet();
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = Sets.newHashSet();
    options.add(CATALOG_NAME);
    options.add(CATALOG_DATABASE);
    options.add(CATALOG_TABLE);
    return options;
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_IDENTIFIER;
  }


  private void validateOptions(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    FactoryUtil.validateFactoryOptions(this, helper.getOptions());
  }

  TableLoader createTableLoader(CatalogTable catalogTable, ObjectPath objectPath, ClassLoader classLoader) {

    TableLoader tableLoader;
    if (catalog != null) {
      tableLoader = createTableLoader(catalog, objectPath);
    } else {
      tableLoader = createTableLoader(catalogTable, catalogTable.getOptions(), objectPath, classLoader);
    }
    return tableLoader;
  }

  private static TableLoader createTableLoader(CatalogBaseTable catalogBaseTable,
                                               Map<String, String> tableProps,
                                               ObjectPath objectPath,
                                               ClassLoader classLoader) {
    Configuration flinkConf = Configuration.fromMap(tableProps);

    String catalogName = flinkConf.getString(CATALOG_NAME);
    Preconditions.checkNotNull(catalogName, "Table property '%s' cannot be null", CATALOG_NAME.key());

    String catalogDatabase = flinkConf.getString(CATALOG_DATABASE, objectPath.getDatabaseName());
    String catalogTable = flinkConf.getString(CATALOG_TABLE, objectPath.getObjectName());

    // make sure we are passing in FlinkCatalogFactory's identifier
    tableProps.put(CommonCatalogOptions.CATALOG_TYPE.key(), FlinkCatalogFactoryOptions.IDENTIFIER);
    FlinkCatalog flinkCatalog =
        (FlinkCatalog) FactoryUtil.createCatalog(catalogName, tableProps, null, classLoader);

    // Create database if not exists in the external catalog.
    if (!flinkCatalog.databaseExists(catalogDatabase)) {
      try {
        flinkCatalog.createDatabase(catalogDatabase, new CatalogDatabaseImpl(Maps.newHashMap(), null), true);
      } catch (DatabaseAlreadyExistException e) {
        throw new AlreadyExistsException(e, "Database %s already exists in the iceberg catalog %s.", catalogName,
            catalogDatabase);
      }
    }

    // Create table if not exists in the external catalog.
    if (!flinkCatalog.tableExists(objectPath)) {
      try {
        flinkCatalog.createIcebergTable(objectPath, catalogBaseTable, true);
      } catch (TableAlreadyExistException e) {
        throw new AlreadyExistsException(e, "Table %s already exists in the database %s and catalog %s",
            catalogTable, catalogDatabase, catalogName);
      }
    }

    return TableLoader.fromCatalog(flinkCatalog.getCatalogLoader(), TableIdentifier.of(catalogDatabase, catalogTable));
  }

  private static TableLoader createTableLoader(FlinkCatalog catalog, ObjectPath objectPath) {
    Preconditions.checkNotNull(catalog, "Flink catalog cannot be null");
    return TableLoader.fromCatalog(catalog.getCatalogLoader(), catalog.toIdentifier(objectPath));
  }
}
