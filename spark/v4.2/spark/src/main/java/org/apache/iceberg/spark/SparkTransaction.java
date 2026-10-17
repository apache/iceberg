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
package org.apache.iceberg.spark;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.transactions.TransactionInfo;
import org.apache.spark.sql.connector.read.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Spark 4.2 DSv2 transaction backed by staged Iceberg transactions.
 *
 * <p>Tables loaded through {@link #catalog()} resolve to the transaction's staged state, so reads
 * observe the transaction's own writes while nothing is visible to other readers until {@link
 * #commit()}. Commit-time conflict detection is delegated to Iceberg's snapshot-isolation checks.
 */
class SparkTransaction implements org.apache.spark.sql.connector.catalog.transactions.Transaction {
  private static final Logger LOG = LoggerFactory.getLogger(SparkTransaction.class);

  private enum State {
    ACTIVE,
    COMMITTED,
    ABORTED,
    CLOSED
  }

  private final SparkCatalog catalog;
  private final TransactionInfo info;
  private final Map<TableIdentifier, Transaction> stagedTransactions = Maps.newHashMap();
  private final Set<Scan> readSet = Sets.newHashSet();
  private State state = State.ACTIVE;

  SparkTransaction(SparkCatalog catalog, TransactionInfo info) {
    this.catalog = catalog;
    this.info = info;
  }

  @Override
  public CatalogPlugin catalog() {
    return new TransactionSparkCatalog(catalog, this);
  }

  /** Returns the staged Iceberg transaction for a table, creating it on first use. */
  Transaction transactionFor(Identifier ident) {
    checkActive();
    TableIdentifier tableIdent = catalog.icebergIdentifier(ident);
    return stagedTransactions.computeIfAbsent(
        tableIdent,
        id -> {
          LOG.debug("Staging transaction for table {} (txn {})", id, info.id());
          return catalog.icebergCatalog().loadTable(id).newTransaction();
        });
  }

  @Override
  public void commit() {
    checkActive();
    // If a table commit fails midway, tables committed before the failure stay committed.
    for (Map.Entry<TableIdentifier, Transaction> entry : stagedTransactions.entrySet()) {
      LOG.debug("Committing staged transaction for table {} (txn {})", entry.getKey(), info.id());
      entry.getValue().commitTransaction();
    }
    state = State.COMMITTED;
  }

  @Override
  public void abort() {
    checkActive();
    stagedTransactions.clear();
    state = State.ABORTED;
  }

  @Override
  public boolean registerScans(Scan[] scans) {
    checkActive();
    for (Scan scan : scans) {
      readSet.add(scan);
    }
    return true;
  }

  @Override
  public void close() {
    if (state == State.ACTIVE) {
      abort();
    }
    state = State.CLOSED;
  }

  private void checkActive() {
    if (state != State.ACTIVE) {
      throw new IllegalStateException(
          "Transaction " + info.id() + " is no longer active (state: " + state + ")");
    }
  }
}
