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
package org.apache.iceberg.hive;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class MetastoreUtil {

  // this class is unique to Hive3 and cannot be found in Hive2, therefore a good proxy to see if
  // we are working against Hive3 dependencies
  private static final String HIVE3_UNIQUE_CLASS =
      "org.apache.hadoop.hive.serde2.io.DateWritableV2";

  private static final DynMethods.UnboundMethod ALTER_TABLE =
      DynMethods.builder("alter_table")
          .impl(
              IMetaStoreClient.class,
              "alter_table_with_environmentContext",
              String.class,
              String.class,
              Table.class,
              EnvironmentContext.class)
          .impl(
              IMetaStoreClient.class,
              "alter_table",
              String.class,
              String.class,
              Table.class,
              EnvironmentContext.class)
          .impl(IMetaStoreClient.class, "alter_table", String.class, String.class, Table.class)
          .build();

  private static final boolean HIVE3_PRESENT_ON_CLASSPATH = detectHive3();

  private MetastoreUtil() {}

  /** Returns true if Hive3 dependencies are found on classpath, false otherwise. */
  public static boolean hive3PresentOnClasspath() {
    return HIVE3_PRESENT_ON_CLASSPATH;
  }

  /**
   * Calls alter_table method using the metastore client. If possible, an environmental context will
   * be used that turns off stats updates to avoid recursive listing.
   */
  public static void alterTable(
      IMetaStoreClient client, String databaseName, String tblName, Table table) {
    EnvironmentContext envContext =
        new EnvironmentContext(
            ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE));
    ALTER_TABLE.invoke(client, databaseName, tblName, table, envContext);
  }

  private static boolean detectHive3() {
    try {
      Class.forName(HIVE3_UNIQUE_CLASS);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
