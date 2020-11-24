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

import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;

public class SparkActions extends Actions {
  protected SparkActions(SparkSession spark, Table table) {
    super(spark, table);
  }

  public static CreateAction migrate(String tableName) {
    return migrate(SparkSession.active(), tableName);
  }

  public static CreateAction migrate(SparkSession spark, String tableName) {
    Spark3Util.CatalogAndIdentifier catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, tableName);

    return new Spark3MigrateAction(spark, catalogAndIdentifier.catalog(), catalogAndIdentifier.identifier());
  }

  public static CreateAction snapshot(String sourceId, String destId) {
    return snapshot(SparkSession.active(), sourceId, destId);
  }

  public static CreateAction snapshot(SparkSession spark, String sourceId, String destId) {
    Spark3Util.CatalogAndIdentifier sourceIdent = Spark3Util.catalogAndIdentifier(spark, sourceId);
    Spark3Util.CatalogAndIdentifier destIdent = Spark3Util.catalogAndIdentifier(spark, destId);
    Preconditions.checkArgument(sourceIdent != destIdent || sourceIdent.catalog() != destIdent.catalog(),
        "Cannot create a snapshot with the same name as the source of the snapshot.");
    return new Spark3SnapshotAction(spark, sourceIdent.catalog(), sourceIdent.identifier(), destIdent.catalog(),
        destIdent.identifier());
  }

  public static CreateAction snapshot(String sourceId, String destId, String location) {
    return snapshot(SparkSession.active(), sourceId, destId, location);
  }

  public static CreateAction snapshot(SparkSession spark, String sourceId, String destId, String location) {
    Spark3Util.CatalogAndIdentifier sourceIdent = Spark3Util.catalogAndIdentifier(spark, sourceId);
    Spark3Util.CatalogAndIdentifier destIdent = Spark3Util.catalogAndIdentifier(spark, destId);
    Preconditions.checkArgument(sourceIdent != destIdent || sourceIdent.catalog() != destIdent.catalog(),
        "Cannot create a snapshot with the same name as the source of the snapshot.");
    return new Spark3SnapshotAction(spark, sourceIdent.catalog(), sourceIdent.identifier(), destIdent.catalog(),
        destIdent.identifier(), location);
  }
}
