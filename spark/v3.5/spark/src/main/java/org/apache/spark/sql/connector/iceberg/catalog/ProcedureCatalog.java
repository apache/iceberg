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

import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * A catalog API for working with stored procedures.
 *
 * <p>Implementations should implement this interface if they expose stored procedures that can be
 * called via CALL statements.
 */
public interface ProcedureCatalog extends CatalogPlugin {
  /**
   * Load a {@link Procedure stored procedure} by {@link Identifier identifier}.
   *
   * @param ident a stored procedure identifier
   * @return the stored procedure's metadata
   * @throws NoSuchProcedureException if there is no matching stored procedure
   */
  Procedure loadProcedure(Identifier ident) throws NoSuchProcedureException;
}
