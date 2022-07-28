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

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

/**
 * An implementation of {@link ActionsProvider} for Spark.
 *
 * <p>This class is the primary API for interacting with actions in Spark that users should use to
 * instantiate particular actions.
 */
public class SparkActions extends BaseSparkActions {

  private SparkActions(SparkSession spark) {
    super(spark);
  }

  public static SparkActions get(SparkSession spark) {
    return new SparkActions(spark);
  }

  public static SparkActions get() {
    return new SparkActions(SparkSession.active());
  }

  @Override
  public SnapshotTable snapshotTable(String tableIdent) {
    String ctx = "snapshot source";
    CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
    CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(ctx, spark(), tableIdent, defaultCatalog);
    return new BaseSnapshotTableSparkAction(
        spark(), catalogAndIdent.catalog(), catalogAndIdent.identifier());
  }

  @Override
  public MigrateTable migrateTable(String tableIdent) {
    String ctx = "migrate target";
    CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
    CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(ctx, spark(), tableIdent, defaultCatalog);
    return new BaseMigrateTableSparkAction(
        spark(), catalogAndIdent.catalog(), catalogAndIdent.identifier());
  }

  @Override
  public RewriteDataFiles rewriteDataFiles(Table table) {
    return new BaseRewriteDataFilesSpark3Action(spark(), table);
  }
}
