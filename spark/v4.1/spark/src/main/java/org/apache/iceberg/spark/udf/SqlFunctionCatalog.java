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
package org.apache.iceberg.spark.udf;

import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Spark-only SPI for catalogs that can expose SQL UDF specifications.
 *
 * <p>This is intentionally read-only for the initial upstream, supporting list/load only. CRUD
 * methods can be added in a follow-up.
 */
public interface SqlFunctionCatalog extends FunctionCatalog {

  /**
   * Load a SQL function specification by identifier.
   *
   * @param ident function identifier
   * @return a parsed SqlFunctionSpec
   * @throws NoSuchFunctionException if the function is not found
   */
  SqlFunctionSpec loadSqlFunction(Identifier ident) throws NoSuchFunctionException;
}
