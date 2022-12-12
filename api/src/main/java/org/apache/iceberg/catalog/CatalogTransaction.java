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
package org.apache.iceberg.catalog;

public interface CatalogTransaction {

  enum IsolationLevel {

    /**
     * All reads that are being made will see the last committed values that existed when the table
     * was loaded inside the catalog transaction. Will successfully commit only if the values
     * updated by the transaction do not conflict with other concurrent updates. <br>
     * <br>
     *
     * <p>Note that under SNAPSHOT isolation a <b>write skew anomaly</b> is acceptable and
     * permitted. In a <b>write skew anomaly</b>, two transactions (T1 and T2) concurrently read an
     * overlapping data set (e.g. values V1 and V2), concurrently make disjoint updates (e.g. T1
     * updates V1, T2 updates V2), and finally concurrently commit, neither having seen the update
     * performed by the other.
     */
    SNAPSHOT,

    /**
     * All reads that are being made will see the last committed values that existed when the table
     * was loaded inside the catalog transaction. All tables participating in the transaction must
     * be in the same state when committing compared to when the table was loaded first within the
     * catalog transaction.<br>
     * <br>
     *
     * <p>Note that a <b>write skew anomaly</b> is not possible under SERIALIZABLE isolation, where
     * two transactions (T1 and T2) concurrently read an overlapping data set (e.g. values V1 and
     * V2), concurrently make disjoint updates (e.g. T1 updates V1, T2 updates V2). This is because
     * under SERIALIZABLE isolation either T1 or T2 would have to occur first and be visible to the
     * other transaction.
     */
    SERIALIZABLE;
  }

  /**
   * Performs an atomic commit of all the pending changes across multiple tables. Engine-specific
   * implementations must ensure that all pending changes are applied atomically.
   */
  void commitTransaction();

  /** Rolls back any pending changes across tables. */
  void rollback();

  /**
   * Returns this catalog transaction as a {@link Catalog} API so that any actions that are called
   * through this API are participating in this catalog transaction.
   *
   * @return This catalog transaction as a {@link Catalog} API. Any actions that are called through
   *     this API are participating in this catalog transaction.
   */
  Catalog asCatalog();

  /**
   * Returns the current {@link IsolationLevel} for this transaction.
   *
   * @return The {@link IsolationLevel} for this transaction.
   */
  IsolationLevel isolationLevel();
}
