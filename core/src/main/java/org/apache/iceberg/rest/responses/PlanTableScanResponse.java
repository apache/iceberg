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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTResponse;

public class PlanTableScanResponse implements RESTResponse {
  private PlanStatus planStatus;
  private String planId;
  private List<String> planTasks;
  private List<FileScanTask> fileScanTasks;
  private List<DeleteFile> deleteFiles;

  private PlanTableScanResponse(
      PlanStatus planStatus,
      String planId,
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles) {
    this.planStatus = planStatus;
    this.planId = planId;
    this.planTasks = planTasks;
    this.fileScanTasks = fileScanTasks;
    this.deleteFiles = deleteFiles;
  }

  public PlanStatus planStatus() {
    return planStatus;
  }

  public String planId() {
    return planId;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("planStatus", planStatus)
        .add("planId", planId)
        .add("planTasks", planTasks)
        .add("fileScanTasks", fileScanTasks)
        .add("deleteFiles", deleteFiles)
        .toString();
  }

  @Override
  public void validate() {
    // validation logic to be performed in PlanTableScanResponseParser
  }

  public static class Builder {
    public Builder() {}

    private PlanStatus planStatus;
    private String planId;
    private List<String> planTasks;
    private List<FileScanTask> fileScanTasks;
    private List<DeleteFile> deleteFiles;

    public Builder withPlanStatus(PlanStatus withPlanStatus) {
      this.planStatus = withPlanStatus;
      return this;
    }

    public Builder withPlanId(String withPlanId) {
      this.planId = withPlanId;
      return this;
    }

    public Builder withPlanTasks(List<String> withPlanTasks) {
      this.planTasks = withPlanTasks;
      return this;
    }

    public Builder withFileScanTasks(List<FileScanTask> withFileScanTasks) {
      this.fileScanTasks = withFileScanTasks;
      return this;
    }

    public Builder withDeleteFiles(List<DeleteFile> withDeleteFiles) {
      this.deleteFiles = withDeleteFiles;
      return this;
    }

    public PlanTableScanResponse build() {
      return new PlanTableScanResponse(planStatus, planId, planTasks, fileScanTasks, deleteFiles);
    }
  }
}
