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

package org.apache.iceberg.actions;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.JobGroupUtils;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

abstract class Spark3CreateAction implements CreateAction {
  private static final Set<String> ALLOWED_SOURCES = ImmutableSet.of("parquet", "avro", "orc", "hive");
  protected static final String LOCATION = "location";
  protected static final String ICEBERG_METADATA_FOLDER = "metadata";
  protected static final List<String> EXCLUDED_PROPERTIES =
      ImmutableList.of("path", "transient_lastDdlTime", "serialization.format");

  private final SparkSession spark;

  // Source Fields
  private final V1Table sourceTable;
  private final CatalogTable sourceCatalogTable;
  private final String sourceTableLocation;
  private final TableCatalog sourceCatalog;
  private final Identifier sourceTableIdent;

  // Destination Fields
  private final StagingTableCatalog destCatalog;
  private final Identifier destTableIdent;

  // Optional Parameters for destination
  private final Map<String, String> additionalProperties = Maps.newHashMap();

  Spark3CreateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent,
                     CatalogPlugin destCatalog, Identifier destTableIdent) {

    this.spark = spark;
    this.sourceCatalog = checkSourceCatalog(sourceCatalog);
    this.sourceTableIdent = sourceTableIdent;
    this.destCatalog = checkDestinationCatalog(destCatalog);
    this.destTableIdent = destTableIdent;

    try {
      this.sourceTable = (V1Table) this.sourceCatalog.loadTable(sourceTableIdent);
      this.sourceCatalogTable = sourceTable.v1Table();
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new NoSuchTableException("Cannot not find source table '%s'", sourceTableIdent);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(String.format("Cannot use non-v1 table '%s' as a source", sourceTableIdent),
          e);
    }
    validateSourceTable();

    this.sourceTableLocation = CatalogUtils.URIToString(sourceCatalogTable.storage().locationUri().get());
  }

  @Override
  public CreateAction withProperties(Map<String, String> properties) {
    this.additionalProperties.putAll(properties);
    return this;
  }

  @Override
  public CreateAction withProperty(String key, String value) {
    this.additionalProperties.put(key, value);
    return this;
  }

  protected SparkSession spark() {
    return spark;
  }

  protected String sourceTableLocation() {
    return sourceTableLocation;
  }

  protected CatalogTable v1SourceTable() {
    return sourceCatalogTable;
  }

  protected TableCatalog sourceCatalog() {
    return sourceCatalog;
  }

  protected Identifier sourceTableIdent() {
    return sourceTableIdent;
  }

  protected StagingTableCatalog destCatalog() {
    return destCatalog;
  }

  protected Identifier destTableIdent() {
    return destTableIdent;
  }

  protected Map<String, String> additionalProperties() {
    return additionalProperties;
  }

  private void validateSourceTable() {
    String sourceTableProvider = sourceCatalogTable.provider().get().toLowerCase(Locale.ROOT);
    Preconditions.checkArgument(ALLOWED_SOURCES.contains(sourceTableProvider),
        "Cannot create an Iceberg table from source provider: '%s'", sourceTableProvider);
    Preconditions.checkArgument(!sourceCatalogTable.storage().locationUri().isEmpty(),
        "Cannot create an Iceberg table from a source without an explicit location");
  }

  private StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {
    Preconditions.checkArgument(catalog instanceof SparkSessionCatalog || catalog instanceof SparkCatalog,
        "Cannot create Iceberg table in non-Iceberg Catalog. " +
            "Catalog '%s' was of class '%s' but '%s' or '%s' are required",
        catalog.name(), catalog.getClass().getName(), SparkSessionCatalog.class.getName(),
        SparkCatalog.class.getName());

    return (StagingTableCatalog) catalog;
  }

  protected StagedSparkTable stageDestTable() {
    try {
      Map<String, String> props = targetTableProps();
      StructType schema = sourceTable.schema();
      Transform[] partitioning = sourceTable.partitioning();
      return (StagedSparkTable) destCatalog.stageCreate(destTableIdent, schema, partitioning, props);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException e) {
      throw new NoSuchNamespaceException("Cannot create a table '%s' because the namespace does not exist",
          destTableIdent);
    } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException e) {
      throw new AlreadyExistsException("Cannot create table '%s' because it already exists", destTableIdent);
    }
  }

  protected void ensureNameMappingPresent(Table table) {
    if (!table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
      NameMapping nameMapping = MappingUtil.create(table.schema());
      String nameMappingJson = NameMappingParser.toJson(nameMapping);
      table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, nameMappingJson).commit();
    }
  }

  protected String getMetadataLocation(Table table) {
    return table.properties().getOrDefault(TableProperties.WRITE_METADATA_LOCATION,
        table.location() + "/" + ICEBERG_METADATA_FOLDER);
  }

  protected abstract Map<String, String> targetTableProps();

  protected abstract TableCatalog checkSourceCatalog(CatalogPlugin catalog);

  protected <T> T withJobGroupInfo(JobGroupInfo info, Supplier<T> supplier) {
    SparkContext context = spark().sparkContext();
    JobGroupInfo previousInfo = JobGroupUtils.getJobGroupInfo(context);
    try {
      JobGroupUtils.setJobGroupInfo(context, info);
      return supplier.get();
    } finally {
      JobGroupUtils.setJobGroupInfo(context, previousInfo);
    }
  }

}
