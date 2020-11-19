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

import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergMetaHook implements HiveMetaHook {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergMetaHook.class);
  private static final Set<String> PARAMETERS_TO_REMOVE = ImmutableSet
      .of(InputFormatConfig.TABLE_SCHEMA, InputFormatConfig.PARTITION_SPEC, Catalogs.LOCATION, Catalogs.NAME);
  private static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet
      .of(InputFormatConfig.EXTERNAL_TABLE_PURGE, hive_metastoreConstants.META_TABLE_STORAGE, "EXTERNAL",
          "bucketing_version");

  private final Configuration conf;
  private Table icebergTable = null;
  private Properties catalogProperties;
  private boolean deleteIcebergTable;
  private FileIO deleteIo;
  private TableMetadata deleteMetadata;

  public HiveIcebergMetaHook(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void preCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    this.catalogProperties = getCatalogProperties(hmsTable);

    // Set the table type even for non HiveCatalog based tables
    hmsTable.getParameters().put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase());

    if (!Catalogs.hiveCatalog(conf)) {
      // If not using HiveCatalog check for existing table
      try {
        this.icebergTable = Catalogs.loadTable(conf, catalogProperties);

        Preconditions.checkArgument(catalogProperties.getProperty(InputFormatConfig.TABLE_SCHEMA) == null,
            "Iceberg table already created - can not use provided schema");
        Preconditions.checkArgument(catalogProperties.getProperty(InputFormatConfig.PARTITION_SPEC) == null,
            "Iceberg table already created - can not use provided partition specification");

        Schema hmsSchema = HiveSchemaUtil.schema(hmsTable.getSd().getCols());
        Preconditions.checkArgument(HiveSchemaUtil.compatible(hmsSchema, icebergTable.schema()),
            "Iceberg table already created - with different specification");

        LOG.info("Iceberg table already exists {}", icebergTable);

        return;
      } catch (NoSuchTableException nte) {
        // If the table does not exist we will create it below
      }
    }

    // If the table does not exist collect data for table creation
    // - InputFormatConfig.TABLE_SCHEMA, InputFormatConfig.PARTITION_SPEC takes precedence so the user can override the
    // Iceberg schema and specification generated by the code
    // - Partitioned Hive tables are currently not allowed

    Schema schema = schema(catalogProperties, hmsTable);
    PartitionSpec spec = spec(schema, catalogProperties, hmsTable);

    catalogProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(schema));
    catalogProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(spec));

    // Allow purging table data if the table is created now and not set otherwise
    if (hmsTable.getParameters().get(InputFormatConfig.EXTERNAL_TABLE_PURGE) == null) {
      hmsTable.getParameters().put(InputFormatConfig.EXTERNAL_TABLE_PURGE, "TRUE");
    }

    // If the table is not managed by Hive catalog then the location should be set
    if (!Catalogs.hiveCatalog(conf)) {
      Preconditions.checkArgument(hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null,
          "Table location not set");
    }

    // Remove creation related properties
    PARAMETERS_TO_REMOVE.forEach(hmsTable.getParameters()::remove);
  }

  @Override
  public void rollbackCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitCreateTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    if (icebergTable == null) {
      if (Catalogs.hiveCatalog(conf)) {
        catalogProperties.put(TableProperties.ENGINE_HIVE_ENABLED, true);
      }

      Catalogs.createTable(conf, catalogProperties);
    }
  }

  @Override
  public void preDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    this.catalogProperties = getCatalogProperties(hmsTable);
    this.deleteIcebergTable = hmsTable.getParameters() != null &&
        "TRUE".equalsIgnoreCase(hmsTable.getParameters().get(InputFormatConfig.EXTERNAL_TABLE_PURGE));

    if (deleteIcebergTable && Catalogs.hiveCatalog(conf)) {
      // Store the metadata and the id for deleting the actual table data
      String metadataLocation = hmsTable.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      this.deleteIo = Catalogs.loadTable(conf, catalogProperties).io();
      this.deleteMetadata = TableMetadataParser.read(deleteIo, metadataLocation);
    }
  }

  @Override
  public void rollbackDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    // do nothing
  }

  @Override
  public void commitDropTable(org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean deleteData) {
    if (deleteData && deleteIcebergTable) {
      if (!Catalogs.hiveCatalog(conf)) {
        LOG.info("Dropping with purge all the data for table {}.{}", hmsTable.getDbName(), hmsTable.getTableName());
        Catalogs.dropTable(conf, catalogProperties);
      } else {
        CatalogUtil.dropTableData(deleteIo, deleteMetadata);
      }
    }
  }

  /**
   * Calculates the properties we would like to send to the catalog.
   * <ul>
   * <li>The base of the properties is the properties store at the Hive Metastore for the given table
   * <li>We add the {@link Catalogs#LOCATION} as the table location
   * <li>We add the {@link Catalogs#NAME} as TableIdentifier defined by the database name and table name
   * <li>We remove the Hive Metastore specific parameters
   * </ul>
   * @param hmsTable Table for which we are calculating the properties
   * @return The properties we can provide for Iceberg functions, like {@link Catalogs}
   */
  private static Properties getCatalogProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Properties properties = new Properties();
    properties.putAll(hmsTable.getParameters());

    if (properties.get(Catalogs.LOCATION) == null &&
        hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null) {
      properties.put(Catalogs.LOCATION, hmsTable.getSd().getLocation());
    }

    if (properties.get(Catalogs.NAME) == null) {
      properties.put(Catalogs.NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());
    }

    // Remove creation related properties
    PROPERTIES_TO_REMOVE.forEach(properties::remove);

    return properties;
  }

  private static Schema schema(Properties properties, org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Schema hmsSchema = HiveSchemaUtil.schema(hmsTable.getSd().getCols());
    if (properties.getProperty(InputFormatConfig.TABLE_SCHEMA) != null) {
      Schema propertySchema = SchemaParser.fromJson(properties.getProperty(InputFormatConfig.TABLE_SCHEMA));
      Preconditions.checkArgument(HiveSchemaUtil.compatible(hmsSchema, propertySchema),
          "Hive and Iceberg table schema should match");
      return propertySchema;
    } else {
      return HiveSchemaUtil.schema(hmsTable.getSd().getCols());
    }
  }

  private static PartitionSpec spec(Schema schema, Properties properties,
      org.apache.hadoop.hive.metastore.api.Table hmsTable) {

    Preconditions.checkArgument(hmsTable.getPartitionKeys() == null || hmsTable.getPartitionKeys().isEmpty(),
        "Partitioned Hive tables are currently not supported");

    if (properties.getProperty(InputFormatConfig.PARTITION_SPEC) != null) {
      return PartitionSpecParser.fromJson(schema, properties.getProperty(InputFormatConfig.PARTITION_SPEC));
    } else {
      return PartitionSpec.unpartitioned();
    }
  }
}
