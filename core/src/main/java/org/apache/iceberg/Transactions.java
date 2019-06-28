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

import com.google.common.base.Preconditions;

public final class Transactions {
  private Transactions() {}

  public static Transaction replaceTableTransaction(TableOperations ops, TableMetadata start) {
    return new BaseTransaction(ops, start);
  }

  public static Transaction createTableTransaction(TableOperations ops, TableMetadata start) {
    Preconditions.checkArgument(ops.current() == null,
            "Cannot start create table transaction: table already exists");
    return new BaseTransaction(ops, start);
  }

  public static Transaction newTransaction(TableOperations ops) {
    return new BaseTransaction(ops, ops.refresh());
  }
}
