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
import org.apache.iceberg.rest.RESTResponse;

public abstract class BaseScanTaskResponse implements RESTResponse {

  private final List<String> planTasks;
  private final List<FileScanTask> fileScanTasks;
  private final List<DeleteFile> deleteFiles;
  private final Map<Integer, PartitionSpec> specsById;

  protected BaseScanTaskResponse(
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> specsById) {
    this.planTasks = planTasks;
    this.fileScanTasks = fileScanTasks;
    this.deleteFiles = deleteFiles;
    this.specsById = specsById;
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

  public abstract static class Builder<B extends Builder<B, R>, R extends BaseScanTaskResponse> {
    private List<String> planTasks;
    private List<FileScanTask> fileScanTasks;
    private List<DeleteFile> deleteFiles;
    private Map<Integer, PartitionSpec> specsById;

    protected Builder() {}

    @SuppressWarnings("unchecked")
    public B self() {
      return (B) this;
    }

    public B withPlanTasks(List<String> tasks) {
      this.planTasks = tasks;
      return self();
    }

    public B withFileScanTasks(List<FileScanTask> tasks) {
      this.fileScanTasks = tasks;
      return self();
    }

    public B withDeleteFiles(List<DeleteFile> deleteFilesList) {
      this.deleteFiles = deleteFilesList;
      return self();
    }

    public B withSpecsById(Map<Integer, PartitionSpec> specs) {
      this.specsById = specs;
      return self();
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

    public abstract R build();
  }
}
