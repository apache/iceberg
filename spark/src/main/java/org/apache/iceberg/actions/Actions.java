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
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.spark.sql.SparkSession;

public class Actions {

  private static final boolean isSpark3 = DynClasses.builder()
      .impl("org.apache.iceberg.spark.Spark3Util").orNull().build() != null;
  private static final String SPARK_2_ACTIONS = "org.apache.iceberg.actions.Spark2Actions";
  private static final String SPARK_3_ACTIONS = "org.apache.iceberg.actions.Spark3Actions";

  private static ActionsFactoryMethods impl = null;

  private static ActionsFactoryMethods impl() {
    if (impl == null) {
      try {
        Actions.impl = (ActionsFactoryMethods) DynConstructors.builder()
            .hiddenImpl(isSpark3 ? SPARK_3_ACTIONS : SPARK_2_ACTIONS)
            .buildChecked()
            .newInstance();
      } catch (NoSuchMethodException e) {
        throw new UnsupportedOperationException("Cannot find actions implementation");
      }
    }

    return impl;
  }

  public static Actions forTable(Table table) {
    return impl().forTable(table);
  }

  private SparkSession spark;
  private Table table;

  protected Actions(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  public RemoveOrphanFilesAction removeOrphanFiles() {
    return new RemoveOrphanFilesAction(spark, table);
  }

  public RewriteManifestsAction rewriteManifests() {
    return new RewriteManifestsAction(spark, table);
  }

  public RewriteDataFilesAction rewriteDataFiles() {
    return new RewriteDataFilesAction(spark, table);
  }

  public ExpireSnapshotsAction expireSnapshots() {
    return new ExpireSnapshotsAction(spark, table);
  }

  protected SparkSession spark() {
    return spark;
  }

  protected Table table() {
    return table;
  }

}
