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
import org.apache.iceberg.rest.RESTResponse;

public class FetchScanTasksResponse implements RESTResponse {

  private List<String> planTasks;
  private List<FileScanTask> fileScanTasks;
  private List<DeleteFile> deleteFiles;
  private Map<Integer, PartitionSpec> partitionSpecsById;

  private FetchScanTasksResponse(
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> partitionSpecsById) {
    this.planTasks = planTasks;
    this.fileScanTasks = fileScanTasks;
    this.deleteFiles = deleteFiles;
    this.partitionSpecsById = partitionSpecsById;
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

  public Map<Integer, PartitionSpec> partitionSpecsById() {
    return partitionSpecsById;
  }

  public void setPartitionSpecsById(Map<Integer, PartitionSpec> partitionSpecsById) {
    this.partitionSpecsById = partitionSpecsById;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        planTasks != null || fileScanTasks != null,
        "Invalid response: planTasks and fileScanTask can not both be null");
    Preconditions.checkArgument(
        deleteFiles() != null && fileScanTasks() == null,
        "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
  }

  public static class Builder {
    public Builder() {}

    private List<String> planTasks;

    private List<FileScanTask> fileScanTasks;

    private List<DeleteFile> deleteFiles;

    private Map<Integer, PartitionSpec> partitionSpecsById;

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

    public Builder withPartitionSpecsById(Map<Integer, PartitionSpec> withParitionSpecById) {
      this.partitionSpecsById = withParitionSpecById;
      return this;
    }

    public FetchScanTasksResponse build() {
      return new FetchScanTasksResponse(planTasks, fileScanTasks, deleteFiles, partitionSpecsById);
    }
  }
}
