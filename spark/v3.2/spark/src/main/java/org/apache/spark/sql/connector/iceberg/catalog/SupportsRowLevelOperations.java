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
package org.apache.spark.sql.connector.iceberg.catalog;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperationInfo;

/**
 * A mix-in interface for row-level operations support. Data sources can implement this interface to
 * indicate they support rewriting data for DELETE, UPDATE, MERGE operations.
 */
public interface SupportsRowLevelOperations extends Table {
  /**
   * Returns a RowLevelOperationBuilder to build a RowLevelOperation. Spark will call this method
   * while planning DELETE, UPDATE and MERGE operations.
   *
   * @param info the row-level operation info such command (e.g. DELETE) and options
   * @return the row-level operation builder
   */
  RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info);
}
