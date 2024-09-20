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
import javax.annotation.Nullable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;
import org.immutables.value.Value;

@Value.Immutable
public interface PlanTableScanRequest extends RESTRequest {

  @Nullable
  Long snapshotId();

  @Nullable
  List<String> select();

  @Nullable
  Expression filter();

  @Nullable
  @Value.Default
  default Boolean caseSensitive() {
    return true;
  }

  @Nullable
  @Value.Default
  default Boolean useSnapShotSchema() {
    return false;
  }

  @Nullable
  Long startSnapShotId();

  @Nullable
  Long endSnapShotId();

  @Nullable
  List<String> statsFields();

  @Override
  default void validate() {
    Preconditions.checkArgument(
        snapshotId() != null ^ (startSnapShotId() != null && endSnapShotId() != null),
        "Either snapshotId must be provided or both startSnapshotId and endSnapshotId must be provided");
  }
}
