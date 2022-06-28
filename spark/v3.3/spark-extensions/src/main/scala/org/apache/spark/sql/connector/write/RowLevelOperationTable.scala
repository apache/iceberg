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

package org.apache.spark.sql.connector.write

import java.util
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.iceberg.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.iceberg.write.ExtendedLogicalWriteInfo
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An internal v2 table implementation that wraps the original table during DELETE, UPDATE,
 * MERGE operations.
 */
case class RowLevelOperationTable(
    table: Table with SupportsRowLevelOperations,
    operation: RowLevelOperation) extends Table with SupportsRead with SupportsWrite {

  override def name: String = table.name
  override def schema: StructType = table.schema
  override def capabilities: util.Set[TableCapability] = table.capabilities
  override def toString: String = table.toString

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    operation.newScanBuilder(options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    operation.newWriteBuilder(info.asInstanceOf[ExtendedLogicalWriteInfo])
  }
}
