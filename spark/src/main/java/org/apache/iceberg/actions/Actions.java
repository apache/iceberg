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
import org.apache.spark.sql.SparkSession;

public class Actions {

  private SparkSession spark;
  private Table table;

  private Actions(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  public static Actions forTable(SparkSession spark, Table table) {
    return new Actions(spark, table);
  }

  public static Actions forTable(Table table) {
    return new Actions(SparkSession.active(), table);
  }

  public RemoveOrphanFilesAction removeOrphanFiles() {
    return new RemoveOrphanFilesAction(spark, table);
  }

  public RewriteManifestsAction rewriteManifests() {
    return new RewriteManifestsAction(spark, table);
  }
}
