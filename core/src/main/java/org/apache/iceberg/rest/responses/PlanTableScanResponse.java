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
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.PlanStatus;

public class PlanTableScanResponse extends BaseScanTaskResponse {
  private final PlanStatus planStatus;
  private final String planId;

  private PlanTableScanResponse(
      PlanStatus planStatus,
      String planId,
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> specsById) {
    super(planTasks, fileScanTasks, deleteFiles, specsById);
    this.planStatus = planStatus;
    this.planId = planId;
    validate();
  }

  public PlanStatus planStatus() {
    return planStatus;
  }

  public String planId() {
    return planId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("planStatus", planStatus())
        .add("planId", planId())
        .toString();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        planStatus() != null, "Invalid response: plan status must be defined");
    Preconditions.checkArgument(
        planStatus() != PlanStatus.SUBMITTED || planId() != null,
        "Invalid response: plan id should be defined when status is '%s'",
        PlanStatus.SUBMITTED.status());
    Preconditions.checkArgument(
        planStatus() != PlanStatus.CANCELLED,
        "Invalid response: '%s' is not a valid status for planTableScan",
        PlanStatus.CANCELLED.status());
    Preconditions.checkArgument(
        planStatus() == PlanStatus.COMPLETED || (planTasks() == null && fileScanTasks() == null),
        "Invalid response: tasks can only be defined when status is '%s'",
        PlanStatus.COMPLETED.status());
    if (null != planId()) {
      Preconditions.checkArgument(
          planStatus() == PlanStatus.SUBMITTED || planStatus() == PlanStatus.COMPLETED,
          "Invalid response: plan id can only be defined when status is '%s' or '%s'",
          PlanStatus.SUBMITTED.status(),
          PlanStatus.COMPLETED.status());
    }

    if (fileScanTasks() == null || fileScanTasks().isEmpty()) {
      Preconditions.checkArgument(
          (deleteFiles() == null || deleteFiles().isEmpty()),
          "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends BaseScanTaskResponse.Builder<Builder, PlanTableScanResponse> {
    private PlanStatus planStatus;
    private String planId;

    /**
     * @deprecated since 1.11.0, visibility will be reduced in 1.12.0; use {@link
     *     PlanTableScanResponse#builder()} instead.
     */
    @Deprecated
    public Builder() {}

    public Builder withPlanStatus(PlanStatus status) {
      this.planStatus = status;
      return this;
    }

    public Builder withPlanId(String id) {
      this.planId = id;
      return this;
    }

    @Override
    public PlanTableScanResponse build() {
      return new PlanTableScanResponse(
          planStatus, planId, planTasks(), fileScanTasks(), deleteFiles(), specsById());
    }
  }
}
