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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.transactions.Transaction;
import org.apache.spark.sql.connector.catalog.transactions.TransactionInfo;
import org.apache.spark.sql.connector.read.Scan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestSparkTransaction {
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("ns", "tbl");
  private static final Identifier IDENT = Identifier.of(new String[] {"ns"}, "tbl");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private InMemoryCatalog catalog;
  private SparkCatalog sparkCatalog;
  private TransactionInfo info;

  @BeforeEach
  void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    catalog.createTable(TABLE_IDENT, SCHEMA);

    sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.icebergCatalog()).thenReturn(catalog);
    when(sparkCatalog.icebergIdentifier(any(Identifier.class))).thenReturn(TABLE_IDENT);

    info = mock(TransactionInfo.class);
    when(info.id()).thenReturn("txn-1");
  }

  @Test
  void commitMakesStagedChangesVisible() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);
    txn.transactionFor(IDENT).table().updateProperties().set("txn.key", "value").commit();

    assertThat(catalog.loadTable(TABLE_IDENT).properties()).doesNotContainKey("txn.key");

    txn.commit();

    assertThat(catalog.loadTable(TABLE_IDENT).properties()).containsEntry("txn.key", "value");
  }

  @Test
  void abortDiscardsStagedChanges() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);
    txn.transactionFor(IDENT).table().updateProperties().set("txn.key", "value").commit();

    txn.abort();

    assertThat(catalog.loadTable(TABLE_IDENT).properties()).doesNotContainKey("txn.key");
  }

  @Test
  void closeAbortsActiveTransaction() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);
    txn.transactionFor(IDENT).table().updateProperties().set("txn.key", "value").commit();

    txn.close();

    assertThat(catalog.loadTable(TABLE_IDENT).properties()).doesNotContainKey("txn.key");
  }

  @Test
  void commitWithNoStagedTablesSucceeds() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);

    txn.commit();
    txn.close();
  }

  @Test
  void doubleCommitThrows() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);
    txn.commit();

    assertThatThrownBy(txn::commit)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Transaction txn-1 is no longer active (state: COMMITTED)");
  }

  @Test
  void operationsAfterCloseThrow() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);
    Scan scan = mock(Scan.class);
    txn.close();

    assertThatThrownBy(txn::commit)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Transaction txn-1 is no longer active (state: CLOSED)");
    assertThatThrownBy(txn::abort)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Transaction txn-1 is no longer active (state: CLOSED)");
    assertThatThrownBy(() -> txn.transactionFor(IDENT))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Transaction txn-1 is no longer active (state: CLOSED)");
    assertThatThrownBy(() -> txn.registerScans(new Scan[] {scan}))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Transaction txn-1 is no longer active (state: CLOSED)");
  }

  @Test
  void registerScansAcceptedWhileActive() {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);

    assertThat(txn.registerScans(new Scan[] {mock(Scan.class)})).isTrue();
  }

  @Test
  void transactionCatalogLoadsStagedTable() throws Exception {
    SparkTransaction txn = new SparkTransaction(sparkCatalog, info);

    TableCatalog txCatalog = (TableCatalog) txn.catalog();

    assertThat(txCatalog.loadTable(IDENT)).isInstanceOf(SparkTable.class);
  }

  @Test
  void beginTransactionCreatesWorkingTransaction() {
    when(sparkCatalog.beginTransaction(info)).thenCallRealMethod();

    Transaction txn = sparkCatalog.beginTransaction(info);
    txn.commit();
    txn.close();
  }
}
