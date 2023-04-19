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
package org.apache.iceberg;

import org.apache.iceberg.BaseTransaction.TransactionType;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class Transactions {
  private Transactions() {}

  public static Transaction createOrReplaceTableTransaction(
      String tableName, TableOperations ops, TableMetadata start) {
    return new BaseTransaction(tableName, ops, TransactionType.CREATE_OR_REPLACE_TABLE, start);
  }

  public static Transaction replaceTableTransaction(
      String tableName, TableOperations ops, TableMetadata start) {
    return new BaseTransaction(tableName, ops, TransactionType.REPLACE_TABLE, start);
  }

  public static Transaction replaceTableTransaction(
      String tableName, TableOperations ops, TableMetadata start, MetricsReporter reporter) {
    return new BaseTransaction(tableName, ops, TransactionType.REPLACE_TABLE, start, reporter);
  }

  public static Transaction createTableTransaction(
      String tableName, TableOperations ops, TableMetadata start) {
    Preconditions.checkArgument(
        ops.current() == null, "Cannot start create table transaction: table already exists");
    return new BaseTransaction(tableName, ops, TransactionType.CREATE_TABLE, start);
  }

  public static Transaction createTableTransaction(
      String tableName, TableOperations ops, TableMetadata start, MetricsReporter reporter) {
    Preconditions.checkArgument(
        ops.current() == null, "Cannot start create table transaction: table already exists");
    return new BaseTransaction(tableName, ops, TransactionType.CREATE_TABLE, start, reporter);
  }

  public static Transaction newTransaction(String tableName, TableOperations ops) {
    return new BaseTransaction(tableName, ops, TransactionType.SIMPLE, ops.refresh());
  }

  public static Transaction newTransaction(
      String tableName, TableOperations ops, MetricsReporter reporter) {
    return new BaseTransaction(tableName, ops, TransactionType.SIMPLE, ops.refresh(), reporter);
  }
}
