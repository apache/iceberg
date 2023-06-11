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
package org.apache.iceberg.rest.requests;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTRequest;

public class CommitTransactionRequest implements RESTRequest {
  private final List<UpdateTableRequest> tableChanges;

  public CommitTransactionRequest(List<UpdateTableRequest> tableChanges) {
    this.tableChanges = tableChanges;
    validate();
  }

  public List<UpdateTableRequest> tableChanges() {
    return ImmutableList.copyOf(tableChanges);
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(null != tableChanges, "Invalid table changes: null");
    Preconditions.checkArgument(!tableChanges.isEmpty(), "Invalid table changes: empty");
    for (UpdateTableRequest tableChange : tableChanges) {
      Preconditions.checkArgument(
          null != tableChange.identifier(), "Invalid table changes: table identifier required");
    }
  }
}
