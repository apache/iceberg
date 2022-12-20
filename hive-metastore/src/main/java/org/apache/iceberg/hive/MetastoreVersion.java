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

public enum MetastoreVersion {
  HIVE_4("org.apache.hadoop.hive.metastore.api.AlterTableResponse", 4),
  HIVE_3("org.apache.hadoop.hive.metastore.api.GetCatalogResponse", 3),
  HIVE_2("org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse", 2),
  HIVE_1_2("org.apache.hadoop.hive.metastore.api.NotificationEventResponse", 1),
  NOT_SUPPORTED("", 0);

  private final String className;
  private final int order;
  private static final MetastoreVersion current = calculate();

  MetastoreVersion(String className, int order) {
    this.className = className;
    this.order = order;
  }

  public static MetastoreVersion current() {
    return current;
  }

  public static boolean min(MetastoreVersion other) {
    return current.order >= other.order;
  }

  private static MetastoreVersion calculate() {
    if (MetastoreUtil.findClass(HIVE_4.className)) {
      return HIVE_4;
    } else if (MetastoreUtil.findClass(HIVE_3.className)) {
      return HIVE_3;
    } else if (MetastoreUtil.findClass(HIVE_2.className)) {
      return HIVE_2;
    } else if (MetastoreUtil.findClass(HIVE_1_2.className)) {
      return HIVE_1_2;
    } else {
      return NOT_SUPPORTED;
    }
  }
}
