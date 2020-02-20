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

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks;

public class SetLocation implements UpdateLocation {
  private final TableOperations ops;
  private String newLocation;

  public SetLocation(TableOperations ops) {
    this.ops = ops;
    this.newLocation = null;
  }

  @Override
  public UpdateLocation setLocation(String location) {
    this.newLocation = location;
    return this;
  }

  @Override
  public String apply() {
    return newLocation;
  }

  @Override
  public void commit() {
    TableMetadata base = ops.refresh();
    Tasks.foreach(ops)
        .retry(TableProperties.getCommitNumRetries(base.properties()))
        .exponentialBackoff(
            TableProperties.getCommitMinRetryWaitMs(base.properties()),
            TableProperties.getCommitMaxRetryWaitMs(base.properties()),
            TableProperties.getCommitTotalRetryTimeMs(base.properties()),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(taskOps -> taskOps.commit(base, base.updateLocation(newLocation)));
  }
}
