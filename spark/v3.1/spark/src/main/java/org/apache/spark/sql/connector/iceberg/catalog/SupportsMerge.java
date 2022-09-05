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
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;

/**
 * A mix-in interface for Table to indicate that it supports row-level operations.
 *
 * <p>This adds {@link #newMergeBuilder(String, LogicalWriteInfo)} that is used to create a scan and
 * a write for a row-level operation.
 */
public interface SupportsMerge extends Table {
  /**
   * Returns a {@link MergeBuilder} which can be used to create both a scan and a write for a
   * row-level operation. Spark will call this method to configure each data source row-level
   * operation.
   *
   * @param info write info
   * @return a merge builder
   */
  MergeBuilder newMergeBuilder(String operation, LogicalWriteInfo info);
}
