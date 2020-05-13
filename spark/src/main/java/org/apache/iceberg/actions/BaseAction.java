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

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;

abstract class BaseAction<R> implements Action<R> {

  protected abstract Table table();

  protected String metadataTableName(MetadataTableType type) {
    String tableName = table().toString();
    if (tableName.contains("/")) {
      return tableName + "#" + type;
    } else if (tableName.startsWith("hadoop.") || tableName.startsWith("hive.")) {
      // HiveCatalog and HadoopCatalog prepend a logical name which we need to drop for Spark 2.4
      return tableName.replaceFirst("(hadoop\\.)|(hive\\.)", "") + "." + type;
    } else {
      return tableName + "." + type;
    }
  }
}
