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

import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.sources.Filter;

// this should be part of SupportsDelete when merged upstream
public interface ExtendedSupportsDelete extends SupportsDelete {
  /**
   * Checks if it is possible to delete data from a data source table that matches filter
   * expressions.
   *
   * <p>Rows should be deleted from the data source iff all of the filter expressions match. That
   * is, the expressions must be interpreted as a set of filters that are ANDed together.
   *
   * <p>Spark will call this method to check if the delete is possible without significant effort.
   * Otherwise, Spark will try to rewrite the delete operation if the data source table supports
   * row-level operations.
   *
   * @param filters filter expressions, used to select rows to delete when all expressions match
   * @return true if the delete operation can be performed
   */
  default boolean canDeleteWhere(Filter[] filters) {
    return true;
  }
}
