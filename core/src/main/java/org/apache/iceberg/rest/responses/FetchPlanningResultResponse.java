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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.PlanStatus;

public class FetchPlanningResultResponse implements TableScanResponse {
  private final PlanStatus planStatus;
  private final List<String> planTasks;
  private final List<FileScanTask> fileScanTasks;
  private final List<DeleteFile> deleteFiles;
  private final Map<Integer, PartitionSpec> specsById;

  private FetchPlanningResultResponse(
      PlanStatus planStatus,
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> specsById) {
    this.planStatus = planStatus;
    this.planTasks = planTasks;
    this.fileScanTasks = fileScanTasks;
    this.deleteFiles = deleteFiles;
    this.specsById = specsById;
    validate();
  }

  public PlanStatus planStatus() {
    return planStatus;
  }

  public List<String> planTasks() {
    return planTasks;
  }

  public List<FileScanTask> fileScanTasks() {
    return fileScanTasks;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public Map<Integer, PartitionSpec> specsById() {
    return specsById;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(planStatus() != null, "Invalid status: null");
    Preconditions.checkArgument(
        planStatus() == PlanStatus.COMPLETED || (planTasks() == null && fileScanTasks() == null),
        "Invalid response: tasks can only be returned in a 'completed' status");
    if (fileScanTasks() == null || fileScanTasks.isEmpty()) {
      Preconditions.checkArgument(
          (deleteFiles() == null || deleteFiles().isEmpty()),
          "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
    }
  }

  public static class Builder {
    private Builder() {}

    private PlanStatus planStatus;
    private List<String> planTasks;
    private List<FileScanTask> fileScanTasks;
    private List<DeleteFile> deleteFiles;
    private Map<Integer, PartitionSpec> specsById;

    public Builder withPlanStatus(PlanStatus status) {
      this.planStatus = status;
      return this;
    }

    public Builder withPlanTasks(List<String> tasks) {
      this.planTasks = tasks;
      return this;
    }

    public Builder withFileScanTasks(List<FileScanTask> tasks) {
      this.fileScanTasks = tasks;
      return this;
    }

    public Builder withDeleteFiles(List<DeleteFile> deletes) {
      this.deleteFiles = deletes;
      return this;
    }

    public Builder withSpecsById(Map<Integer, PartitionSpec> specs) {
      this.specsById = specs;
      return this;
    }

    public FetchPlanningResultResponse build() {
      return new FetchPlanningResultResponse(
          planStatus, planTasks, fileScanTasks, deleteFiles, specsById);
    }
  }
}