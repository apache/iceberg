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
package org.apache.iceberg.spark.actions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.actions.Hive2Iceberg;
import org.apache.iceberg.actions.ImmutableHive2Iceberg;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hive2IcebergAction extends BaseTableCreationSparkAction<Hive2IcebergAction>
    implements Hive2Iceberg {

  private static final Logger LOG = LoggerFactory.getLogger(Hive2IcebergAction.class);
  private SnapshotTableSparkAction snapshotTableSparkAction;
  private Identifier sourceTableIdent;
  private int parallelism = 1;
  private SparkSession sparkSession;
  private CatalogPlugin sourceCatalog;

  Hive2IcebergAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent) {
    super(spark, sourceCatalog, sourceTableIdent);
    this.sourceTableIdent = sourceTableIdent;
    this.sparkSession = spark;
    this.sourceCatalog = sourceCatalog;
  }

  @Override
  public Hive2Iceberg parallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  @Override
  public Result execute() {

    // Step1. Process SnapShot
    String snapShotTableName = generateSnapShotTableName(sourceTableIdent.name());
    boolean snapshotSuccess = handleSnapshotCreation(snapShotTableName);
    if (!snapshotSuccess) {
      return buildFailureResult("Snapshot creation failed");
    }

    // Step2. Prepare SourceTable
    OperationSourceTableResult sourceTableResult = handleSourceTablePreparation();
    if (!sourceTableResult.isSuccess()) {
      return buildFailureResult("Source Table preparation failed");
    }

    CatalogTable sourceSparkTable = sourceTableResult.getSourceSparkTable();
    Identifier sourceIdentifier = sourceTableResult.getSourceIdentifier();

    // Step3. Prepare TargetTable
    OperationTargetTableResult targetTableResult = handleTargetTablePreparation(snapShotTableName);
    if (!targetTableResult.isSuccess()) {
      return buildFailureResult("Target Table preparation failed");
    }

    SparkTable snapShotSparkTable = targetTableResult.getSnapShotSparkTable();

    // Step4. Rewrite Table Path
    OperationPathRewriteResult handlePathRewrite =
        handlePathRewrite(snapShotSparkTable, sourceSparkTable);
    if (!handlePathRewrite.isSuccess()) {
      return buildFailureResult("Rewrite Table Path failed");
    }

    String latestVersion = handlePathRewrite.getLatestVersion();
    String targetPrefix = CatalogUtils.URIToString(sourceSparkTable.storage().locationUri().get());

    // Step5. MetadataMigration
    boolean metadataMigration =
        handleMetadataMigration(targetPrefix, latestVersion, sourceIdentifier);
    if (!metadataMigration) {
      return buildFailureResult("Metadata Migration failed");
    }

    // Step6. Clear SnapshotTable
    boolean cleanSnapShotTable = handleClearSnapShotTable(snapShotTableName, snapShotSparkTable);
    if (!cleanSnapShotTable) {
      return buildFailureResult("Clean SnapShotTable failed");
    }

    return ImmutableHive2Iceberg.Result.builder().latestVersion(latestVersion).build();
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // currently the import code relies on being able to look up the table in the session catalog
    Preconditions.checkArgument(
        catalog.name().equalsIgnoreCase("spark_catalog"),
        "Cannot snapshot a table that isn't in the session catalog (i.e. spark_catalog). "
            + "Found source catalog: %s.",
        catalog.name());

    Preconditions.checkArgument(
        catalog instanceof TableCatalog,
        "Cannot snapshot as catalog %s of class %s in not a table catalog",
        catalog.name(),
        catalog.getClass().getName());

    return (TableCatalog) catalog;
  }

  @Override
  protected StagingTableCatalog destCatalog() {
    return null;
  }

  @Override
  protected Identifier destTableIdent() {
    return null;
  }

  @Override
  protected Map<String, String> destTableProps() {
    return Map.of();
  }

  @Override
  protected Hive2IcebergAction self() {
    return this;
  }

  /**
   * Step 1 of the migration: Create a snapshot for the table to be migrated.
   *
   * <p>This method automatically generates a snapshot table name using the format:
   * tableName_snapshot_yyyyMMdd_HHmmss, where tableName is the name of the original table, followed
   * by the current timestamp.
   *
   * <p>
   *
   * @return true if the snapshot is successfully created; false otherwise.
   */
  private boolean handleSnapshotCreation(String snapShotTableName) {
    try {
      snapshotTableSparkAction =
          new SnapshotTableSparkAction(sparkSession, sourceCatalog, sourceTableIdent);
      snapshotTableSparkAction = snapshotTableSparkAction.as(snapShotTableName);
      snapshotTableSparkAction =
          snapshotTableSparkAction.executeWith(SparkTableUtil.migrationService(parallelism));
      Map<String, String> properties = Maps.newHashMap();
      SnapshotTable.Result result = snapshotTableSparkAction.tableProperties(properties).execute();
      long dataFilesCount = result.importedDataFilesCount();
      Preconditions.checkArgument(dataFilesCount > 0, "snapShot may have failed.");
      return true;
    } catch (Exception e) {
      LOG.error("Snapshot creation failed", e);
      return false;
    }
  }

  /**
   * Generates a snapshot table name by appending the current timestamp to the given table name. The
   * timestamp is formatted as "yyyyMMdd_HHmmss" (year, month, day, hour, minute, second).
   *
   * @param tableName The base name of the table to which the snapshot suffix will be added.
   * @return The generated snapshot table name, which consists of the original table name followed
   *     by "_snapshot_" and the formatted timestamp.
   */
  private String generateSnapShotTableName(String tableName) {
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    String tableSuffix = now.format(formatter);
    String snapShotTableName = tableName + "_snapshot_" + tableSuffix;
    LOG.info("Snapshot table name: {}", snapShotTableName);
    return snapShotTableName;
  }

  public class OperationSourceTableResult {
    private boolean success;
    private CatalogTable sourceSparkTable;
    private Identifier sourceIdentifier;

    public OperationSourceTableResult(
        boolean pSuccess, CatalogTable pSourceSparkTable, Identifier pSourceIdentifier) {
      this.success = pSuccess;
      this.sourceSparkTable = pSourceSparkTable;
      this.sourceIdentifier = pSourceIdentifier;
    }

    public OperationSourceTableResult(boolean pSuccess) {
      this.success = pSuccess;
    }

    public boolean isSuccess() {
      return success;
    }

    public CatalogTable getSourceSparkTable() {
      return sourceSparkTable;
    }

    public Identifier getSourceIdentifier() {
      return sourceIdentifier;
    }
  }

  /**
   * Step 2: Prepare the source table and return the corresponding CatalogTable and
   * sourceIdentifier.
   *
   * @return An object containing the operation status, the CatalogTable of the source table, * and
   *     the source identifier. Returns true with the source table details if successful, * or false
   *     if the operation fails.
   */
  private OperationSourceTableResult handleSourceTablePreparation() {
    try {
      String ctx = "hive2iceberg source table preparation";
      CatalogPlugin sourceCatalog = sparkSession.sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(
              ctx, sparkSession, sourceTableIdent.name(), sourceCatalog);
      Identifier sourceIdentifier = catalogAndIdent.identifier();
      TableCatalog tableCatalog = checkTargetCatalog(catalogAndIdent.catalog());
      V1Table targetTable = (V1Table) tableCatalog.loadTable(sourceIdentifier);
      CatalogTable sourceSparkTable = targetTable.v1Table();
      LOG.info("hive2iceberg source table {} preparation", sourceSparkTable.qualifiedName());
      return new OperationSourceTableResult(true, sourceSparkTable, sourceIdentifier);
    } catch (Exception e) {
      LOG.error("hive2iceberg source table preparation failed", e);
      return new OperationSourceTableResult(false);
    }
  }

  public class OperationTargetTableResult {
    private boolean success;
    private SparkTable snapShotSparkTable;

    public OperationTargetTableResult(boolean pSuccess, SparkTable pSnapShotSparkTable) {
      this.success = pSuccess;
      this.snapShotSparkTable = pSnapShotSparkTable;
    }

    public OperationTargetTableResult(boolean pSuccess) {
      this.success = pSuccess;
    }

    public boolean isSuccess() {
      return success;
    }

    public SparkTable getSnapShotSparkTable() {
      return snapShotSparkTable;
    }
  }

  /**
   * Step 3: Prepare the target table and return the corresponding SparkTable.
   *
   * @param targetTableName The name of the snapshot table used to load the target table from the
   *     Spark Catalog.
   * @return An object containing the operation status and the target SparkTable. If successful, it
   *     returns true along with the corresponding target table; if it fails, it returns false.
   */
  private OperationTargetTableResult handleTargetTablePreparation(String targetTableName) {
    SparkTable snapShotSparkTable;
    try {
      String ctx = "hive2iceberg target table preparation";
      CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(ctx, spark(), targetTableName, defaultCatalog);
      TableCatalog tableCatalog = checkTargetCatalog(catalogAndIdent.catalog());
      snapShotSparkTable = (SparkTable) tableCatalog.loadTable(catalogAndIdent.identifier());
      LOG.info("hive2iceberg target table {} preparation", snapShotSparkTable.name());
      return new OperationTargetTableResult(true, snapShotSparkTable);
    } catch (Exception e) {
      LOG.error("hive2iceberg target table preparation failed", e);
      return new OperationTargetTableResult(false);
    }
  }

  public class OperationPathRewriteResult {
    private boolean success;
    private String latestVersion;

    public OperationPathRewriteResult(boolean pSuccess, String pLatestVersion) {
      this.success = pSuccess;
      this.latestVersion = pLatestVersion;
    }

    public OperationPathRewriteResult(boolean pSuccess) {
      this.success = pSuccess;
    }

    public boolean isSuccess() {
      return success;
    }

    public String getLatestVersion() {
      return latestVersion;
    }
  }

  /**
   * Step 4: Perform path rewrite and update the metadata of the snapshot table into the original
   * table.
   *
   * @param snapShotTable The snapshot table.
   * @param sourceTable The original table (Hive table).
   * @return An object containing the operation status and the latest version of the path rewrite.
   *     If the path rewrite is successful, it returns true with the corresponding version
   *     information; if it fails, it returns false.
   */
  private OperationPathRewriteResult handlePathRewrite(
      SparkTable snapShotTable, CatalogTable sourceTable) {
    try {
      String sourcePrefix = snapShotTable.table().location();
      String targetPrefix = CatalogUtils.URIToString(sourceTable.storage().locationUri().get());
      String stagingLocation = targetPrefix;

      RewriteTablePathSparkAction rewriteAction =
          SparkActions.get().rewriteTablePath(snapShotTable.table());
      if (stagingLocation != null) {
        rewriteAction.stagingLocation(stagingLocation);
      }
      rewriteAction.hiveMigrate(true);
      RewriteTablePath.Result rewrite =
          rewriteAction.rewriteLocationPrefix(sourcePrefix, targetPrefix).execute();
      return new OperationPathRewriteResult(true, rewrite.latestVersion());
    } catch (Exception e) {
      LOG.error("Path rewrite failed", e);
      return new OperationPathRewriteResult(false);
    }
  }

  /**
   * Step5. Metadata migration step.
   *
   * @param targetPrefix The prefix of the target table's path, used to generate the location for
   *     the target metadata file.
   * @param latestVersion The latest version of the target table, which is used to set the metadata
   *     file's location.
   * @param sourceIdentifier The identifier of the source table, used to identify and update the
   *     source table's metadata.
   * @return Returns true if the metadata migration is successful; otherwise, returns false.
   */
  private boolean handleMetadataMigration(
      String targetPrefix, String latestVersion, Identifier sourceIdentifier) {
    try {
      Map<String, String> properties = Maps.newHashMap();
      properties.put(CatalogProperties.CATALOG_IMPL, HiveCatalog.class.getName());

      HiveCatalog hiveCatalog = new HiveCatalog();
      hiveCatalog.setConf(spark().sparkContext().hadoopConfiguration());
      hiveCatalog.initialize("hive", properties);

      String targetMetadataFileLocation = targetPrefix + "/metadata/" + latestVersion;
      TableCatalog targetSourceCatalog = checkTargetCatalog(sourceCatalog);
      targetSourceCatalog.alterTable(
          sourceIdentifier,
          TableChange.setProperty("write.parquet.compression-codec", "zstd"),
          TableChange.setProperty("metadata_location", targetMetadataFileLocation),
          TableChange.setProperty("table_type", "ICEBERG"),
          TableChange.setProperty("provide", "iceberg"));
      return true;
    } catch (Exception e) {
      LOG.error("Metadata migration failed", e);
      return false;
    }
  }

  private boolean handleClearSnapShotTable(String snapShotTable, SparkTable snapShotSparkTable) {
    try {
      String ctx = "clean snapshot table";
      CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(ctx, spark(), snapShotTable, defaultCatalog);
      String location = snapShotSparkTable.table().location();

      // drop table
      sourceCatalog().dropTable(catalogAndIdent.identifier());

      // clean location
      Configuration conf = new Configuration();
      HadoopFileIO fileIO = new HadoopFileIO(conf);
      fileIO.deletePrefix(location);
      LOG.info("clean hive2iceberg snapshot table: {}.", snapShotSparkTable.name());
      return true;
    } catch (Exception e) {
      LOG.error("clean hive2iceberg snapshot table failed", e);
      return false;
    }
  }

  private TableCatalog checkTargetCatalog(CatalogPlugin catalog) {
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

  private ImmutableHive2Iceberg.Result buildFailureResult(String message) {
    return ImmutableHive2Iceberg.Result.builder().successful(false).failureMessage(message).build();
  }
}
