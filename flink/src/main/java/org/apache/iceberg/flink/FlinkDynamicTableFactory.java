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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class FlinkDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
  private static final String FACTORY_IDENTIFIER = "iceberg";

  public static final ConfigOption<String> CATALOG_NAME =
      ConfigOptions.key("catalog-name")
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog name");

  public static final ConfigOption<String> CATALOG_TYPE =
      ConfigOptions.key(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE)
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog type.");

  public static final ConfigOption<String> CATALOG_DATABASE =
      ConfigOptions.key("catalog-database")
          .stringType()
          .defaultValue(FlinkCatalogFactory.DEFAULT_DATABASE_VALUE)
          .withDeprecatedKeys("Catalog database");

  private final FlinkCatalog catalog;

  public FlinkDynamicTableFactory() {
    this.catalog = null;
  }

  public FlinkDynamicTableFactory(FlinkCatalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    ObjectIdentifier objectIdentifier = context.getObjectIdentifier();
    Map<String, String> tableProps = context.getCatalogTable().getOptions();
    CatalogTable catalogTable = context.getCatalogTable();
    TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

    TableLoader tableLoader;
    if (catalog != null) {
      tableLoader = createTableLoader(catalog, objectIdentifier.toObjectPath());
    } else {
      tableLoader = createTableLoader(catalogTable, tableProps, objectIdentifier.getObjectName());
    }

    return new IcebergTableSource(tableLoader, tableSchema, tableProps, context.getConfiguration());
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    ObjectPath objectPath = context.getObjectIdentifier().toObjectPath();
    Map<String, String> tableProps = context.getCatalogTable().getOptions();
    CatalogTable catalogTable = context.getCatalogTable();
    TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

    TableLoader tableLoader;
    if (catalog != null) {
      tableLoader = createTableLoader(catalog, objectPath);
    } else {
      tableLoader = createTableLoader(catalogTable, tableProps, objectPath.getObjectName());
    }

    return new IcebergTableSink(tableLoader, tableSchema);
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = Sets.newHashSet();
    options.add(CATALOG_TYPE);
    options.add(CATALOG_NAME);
    options.add(CATALOG_DATABASE);
    return Sets.newHashSet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Sets.newHashSet();
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_IDENTIFIER;
  }

  private static TableLoader createTableLoader(CatalogBaseTable catalogTable,
                                               Map<String, String> tableProps,
                                               String tableName) {
    Configuration flinkConf = new Configuration();
    tableProps.forEach(flinkConf::setString);

    String catalogName = flinkConf.getString(CATALOG_NAME);
    Preconditions.checkNotNull(catalogName, "Table property '%s' cannot be null", CATALOG_NAME.key());

    String catalogDatabase = flinkConf.getString(CATALOG_DATABASE);
    Preconditions.checkNotNull(catalogDatabase, "Table property '%s' cannot be null", CATALOG_DATABASE.key());

    org.apache.hadoop.conf.Configuration hadoopConf = FlinkCatalogFactory.clusterHadoopConf();
    CatalogLoader catalogLoader = FlinkCatalogFactory.createCatalogLoader(
        catalogName,
        tableProps,
        hadoopConf
    );

    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    Catalog flinkCatalog = factory.createCatalog(catalogName, tableProps, hadoopConf);
    ObjectPath objectPath = new ObjectPath(catalogDatabase, tableName);
    if (!flinkCatalog.tableExists(objectPath)) {
      try {
        flinkCatalog.createTable(objectPath, catalogTable, true);
      } catch (TableAlreadyExistException | DatabaseNotExistException e) {
        throw new RuntimeException(e);
      }
    }

    return TableLoader.fromCatalog(
        catalogLoader,
        TableIdentifier.of(catalogDatabase, tableName)
    );
  }

  private static TableLoader createTableLoader(FlinkCatalog catalog, ObjectPath objectPath) {
    Preconditions.checkNotNull(catalog, "Flink catalog cannot be null");
    return TableLoader.fromCatalog(catalog.getCatalogLoader(), catalog.toIdentifier(objectPath));
  }
}
