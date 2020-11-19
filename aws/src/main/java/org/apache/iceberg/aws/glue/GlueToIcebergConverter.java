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

package org.apache.iceberg.aws.glue;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Table;

class GlueToIcebergConverter {

  private GlueToIcebergConverter() {
  }

  static Namespace toNamespace(Database database) {
    return Namespace.of(database.name());
  }

  static TableIdentifier toTableId(Table table) {
    return TableIdentifier.of(table.databaseName(), table.name());
  }

  /**
   * Validate the Glue table is Iceberg table by checking its parameters
   * @param table glue table
   * @param fullName full table name for logging
   */
  static void validateTable(Table table, String fullName) {
    String tableType = table.parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    ValidationException.check(tableType != null && tableType.equalsIgnoreCase(
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
        "Input Glue table is not an iceberg table: %s (type=%s)", fullName, tableType);
  }
}
