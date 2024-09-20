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
package org.apache.iceberg.rest.responses;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTResponse;

public interface PlanTableScanResponse extends RESTResponse {
  PlanStatus planStatus();

  @Nullable
  String planId();

  @Nullable
  List<String> planTasks();

  @Nullable
  List<FileScanTask> fileScanTasks();

  @Nullable
  List<DeleteFile> deleteFiles();

  @Override
  default void validate() {
    Preconditions.checkArgument(planStatus() != null, "invalid response, status can not be null");
    Preconditions.checkArgument(
        planStatus() != PlanStatus.CANCELLED,
        "invalid response, 'cancelled' is not a valid status for this endpoint");
    Preconditions.checkArgument(
        planStatus() != PlanStatus.COMPLETED && (planTasks() != null || fileScanTasks() != null),
        "invalid response, tasks can only be returned in a 'completed' status");
  }
}
