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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Objects;

class CommitGroupKey {
  private final String tableName;
  private final String branch;
  private final String jobId;
  private final String operatorId;

  CommitGroupKey(DynamicCommittable committable) {
    this.tableName = committable.key().tableName();
    this.branch = committable.key().branch();
    this.jobId = committable.jobId();
    this.operatorId = committable.operatorId();
  }

  String tableName() {
    return tableName;
  }

  String branch() {
    return branch;
  }

  String jobId() {
    return jobId;
  }

  String operatorId() {
    return operatorId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof CommitGroupKey that)) {
      return false;
    }
    return tableName.equals(that.tableName)
        && branch.equals(that.branch)
        && Objects.equals(jobId, that.jobId)
        && Objects.equals(operatorId, that.operatorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, branch, jobId, operatorId);
  }
}
