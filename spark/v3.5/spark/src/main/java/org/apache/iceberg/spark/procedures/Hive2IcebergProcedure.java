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
package org.apache.iceberg.spark.procedures;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.actions.RewriteTablePathSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hive2IcebergProcedure extends BaseProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(Hive2IcebergProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("source_table", DataTypes.StringType),
        ProcedureParameter.optional("parallelism", DataTypes.IntegerType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("imported_files_count", DataTypes.LongType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<Hive2IcebergProcedure>() {
      @Override
      protected Hive2IcebergProcedure doBuild() {
        return new Hive2IcebergProcedure(tableCatalog());
      }
    };
  }

  private Hive2IcebergProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {

    /////////////////////////////////// Process SnapShot///////////////////////////////////////////

    // 1. Prepare Source Table
    String sourceTable = args.getString(0);
    Preconditions.checkArgument(
        sourceTable != null && !sourceTable.isEmpty(),
        "Cannot handle an empty identifier for argument source_table");

    // 2. Prepare Dest Table
    // Define a time formatting template.
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    String tableSuffix = now.format(formatter);
    String snapShotTable = sourceTable + "_snapshot_" + tableSuffix;
    Preconditions.checkArgument(
        snapShotTable != null && !snapShotTable.isEmpty(),
        "Cannot handle an empty identifier for argument table");

    // 3. Prepare Table Properties
    Map<String, String> properties = Maps.newHashMap();
    Preconditions.checkArgument(
        !sourceTable.equals(snapShotTable),
        "Cannot create a hive2Iceberg snapshot with the same name as the source of the snapshot.");
    SnapshotTable action = SparkActions.get().snapshotTable(sourceTable).as(snapShotTable);

    // 4. Prepare Parallelism
    if (!args.isNullAt(1)) {
      int parallelism = args.getInt(1);
      Preconditions.checkArgument(parallelism > 0, "Parallelism should be larger than 0");
      action = action.executeWith(SparkTableUtil.migrationService(parallelism));
    }

    SnapshotTable.Result result = action.tableProperties(properties).execute();
    long dataFilesCount = result.importedDataFilesCount();
    Preconditions.checkArgument(dataFilesCount > 0, "snapShot may have failed.");

    /////////////////////////////////// Process ReWrite///////////////////////////////////////////

    // 1. Prepare SourceTable
    CatalogTable sourceSparkTable;
    Identifier sourceIdentifier;
    CatalogPlugin sourceCatalog;
    try {
      String ctx = "hive2iceberg source";
      sourceCatalog = spark().sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(ctx, spark(), sourceTable, sourceCatalog);
      sourceIdentifier = catalogAndIdent.identifier();
      TableCatalog tableCatalog = checkTargetCatalog(catalogAndIdent.catalog());
      V1Table targetTable = (V1Table) tableCatalog.loadTable(sourceIdentifier);
      sourceSparkTable = targetTable.v1Table();
      LOG.info("hive2iceberg source table: {}.", sourceSparkTable.qualifiedName());
    } catch (Exception e) {
      LOG.error("parse Source Table Error.", e);
      throw new RuntimeException(e);
    }

    // 2. Prepare TargetTable
    SparkTable snapShotSparkTable;
    try {
      String ctx = "hive2iceberg target";
      CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(ctx, spark(), snapShotTable, defaultCatalog);
      snapShotSparkTable = (SparkTable) tableCatalog().loadTable(catalogAndIdent.identifier());
      LOG.info("hive2iceberg dest table: {}.", snapShotSparkTable.name());
    } catch (Exception e) {
      LOG.error("parse Dest Table Error.", e);
      throw new RuntimeException(e);
    }

    // 3. Rewrite Table Path
    String sourcePrefix = snapShotSparkTable.table().location();
    String targetPrefix = CatalogUtils.URIToString(sourceSparkTable.storage().locationUri().get());
    String stagingLocation = targetPrefix + "/metadata";
    boolean metaMigrate = true;

    RewriteTablePathSparkAction rewriteAction =
        SparkActions.get().rewriteTablePath(snapShotSparkTable.table());
    if (stagingLocation != null) {
      rewriteAction.stagingLocation(stagingLocation);
    }
    rewriteAction.hiveMetaMigrate(metaMigrate);
    RewriteTablePath.Result rewrite =
        rewriteAction.rewriteLocationPrefix(sourcePrefix, targetPrefix).execute();
    String fileListLocation = rewrite.fileListLocation();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileListLocation), "rewrite may have failed.");

    /////////////////////////////////// Update HMS///////////////////////////////////////////

    if (metaMigrate) {
      try {
        properties.put(CatalogProperties.CATALOG_IMPL, HiveCatalog.class.getName());

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark().sparkContext().hadoopConfiguration());
        hiveCatalog.initialize("hive", properties);

        String metadataFileLocation = getMetadataLocation(snapShotSparkTable.table());

        String targetMetadataFileLocation =
            targetPrefix + "/metadata/" + fileName(metadataFileLocation);
        TableCatalog targetSourceCatalog = checkTargetCatalog(sourceCatalog);
        targetSourceCatalog.alterTable(
            sourceIdentifier,
            TableChange.setProperty("metadata_location", targetMetadataFileLocation),
            TableChange.setProperty("table_type", "ICEBERG"),
            TableChange.setProperty("provide", "iceberg"));
      } catch (Exception e) {
        LOG.error("ReWrite metaMigrate Failed.", e);
        throw new RuntimeException(e);
      }
    }

    return new InternalRow[] {newInternalRow(result.importedDataFilesCount())};
  }

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath =
        ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    return currentMetadataPath;
  }

  protected TableCatalog checkTargetCatalog(CatalogPlugin catalog) {
    // currently the import code relies on being able to look up the table in the session catalog
    Preconditions.checkArgument(
        catalog.name().equalsIgnoreCase("spark_catalog"),
        "Cannot rewrite a table that isn't in the session catalog (i.e. spark_catalog). "
            + "Found source catalog: %s.",
        catalog.name());

    Preconditions.checkArgument(
        catalog instanceof TableCatalog,
        "Cannot rewrite as catalog %s of class %s in not a table catalog",
        catalog.name(),
        catalog.getClass().getName());

    return (TableCatalog) catalog;
  }

  protected String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private InternalRow[] toOutputRows(RewriteTablePath.Result result) {
    return new InternalRow[] {
      newInternalRow(
          UTF8String.fromString(result.latestVersion()),
          UTF8String.fromString(result.fileListLocation()))
    };
  }

  @Override
  public String description() {
    return "Hive2IcebergProcedure";
  }
}
