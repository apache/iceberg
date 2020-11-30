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

import java.util.Collections;
import java.util.Set;

/**
 * Catalog methods for working with catalog-level transactions.
 *
 * <p>Catalog implementations are not required to support catalog-level transactional state.
 * If they do, they may support one or more {@code IsolationLevel}s and one or more
 * {@code LockingMode}s.
 */
public interface SupportsCatalogTransactions {

  /**
   * The level of isolation for a transaction.
   */
  enum IsolationLevel {

    /**
     * Only read committed data. Reading the same table multiple times
     * may result in different committed reads for each read.
     */
    READ_COMMITTED,

    /**
     * Only read committed data. Reading the same table multiple times will
     * result in the same view of those tables.
     */
    REPEATED_READ,

    /**
     * Only read committed data. A commit will only succeed if there have
     * been no changes to data read during the course of the transaction
     * prior to the commit operation.
     */
    SERIALIZABLE;
  }

  /**
   * The type of locking mode used by the transaction.
   */
  enum LockingMode {

    /**
     * Pessimistically lock tables. In this situation, each table being worked on will grab a lock.
     * This mode will typically result in a better chance for transactions to complete at the cost
     * of throughput and resource consumption.
     */
    PESSIMISTIC,

    /**
     * Run the transaction with an optimistic behavior. This assumes that most operations will not
     * conflict. It typically increases concurrency and throughput while reducing resource
     * consumption.
     */
    OPTIMISTIC;
  }

  /**
   * Get the list of {@link LockingMode}s this Catalog supports.
   * @return A set of locking modes supported.
   */
  default Set<LockingMode> lockingModes() {
    return Collections.emptySet();
  }

  /**
   * Get the list of {@link IsolationLevel}s this Catalog supports.
   * @return A set of locking modes supported.
   */
  default Set<IsolationLevel> isolationLevels() {
    return Collections.emptySet();
  }

  /**
   * Start a new transaction and return a {@TransactionalCatalog} that supports
   * cross table operations. If a {@link Catalog} supports {@link OPTIMISTIC}
   * locking mode, this will use that mode. If it only supports {@link PESSIMISTIC},
   * it will use that mode with default values for lock times.
   *
   * @param isolationLevel The {@link IsolationLevel} to use for the transaction.
   * @return A Catalog with an open transaction.
   * @throws IllegalArgumentException If the IsolationLevel is not supported.
   */
  TransactionalCatalog createTransaction(IsolationLevel isolationLevel);

  /**
   * Start a new transaction that holds locks for the life of the transaction.
   *
   * <p>If supported, this type of transaction allows a much higher likelihood of
   * transaction completion but can also result in more failures, lower concurrency
   * and deadlocks.
   *
   * @param isolationLevel The {@link IsolationLevel} to use for the transaction.
   * @param lockWaitInMillis How long to wait in milliseconds when grabbing a pessimistic lock.
   * @param maxTimeMillis The maximum amount of time the transaction is allowed to be open.
   * @param tables An optional list of tables that should be locked immediately for this
   *        transaction. Note that this can include tables that exist and tables that do not
   *        exist as this transaction may be adding new tables and want to grab pessimistic locks
   *        for those operations.
   *
   * @return A Catalog with an open transaction.
   *
   * @throws IllegalArgumentException If the IsolationLevel is not supported. Also throws if
   *         {@code lockWaitInMillis} or {@code maxTimeMillis} are not within supported limits
   *         (check specific {@link Catalog} documentation for any limits).
   * @throws CommitFailedException If the list of {@code tables} cannot be locked.
   * @throws ValidationException If the set of tables names includes tables names that are not
   *         valid.
   */
  TransactionalCatalog createLockingTransaction(
      IsolationLevel isolationLevel,
      long lockWaitInMillis,
      long maxTimeMillis,
      TableIdentifier...tables);

}
