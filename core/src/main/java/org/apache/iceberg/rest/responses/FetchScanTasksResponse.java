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

public class FetchScanTasksResponse extends BaseScanTaskResponse {

  private FetchScanTasksResponse(
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> specsById) {
    super(planTasks, fileScanTasks, deleteFiles, specsById);
    validate();
  }

  @Override
  public void validate() {
    if (fileScanTasks() == null || fileScanTasks().isEmpty()) {
      Preconditions.checkArgument(
          (deleteFiles() == null || deleteFiles().isEmpty()),
          "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
    }

    Preconditions.checkArgument(
        planTasks() != null || fileScanTasks() != null,
        "Invalid response: planTasks and fileScanTask cannot both be null");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder
      extends BaseScanTaskResponse.Builder<Builder, FetchScanTasksResponse> {
    private Builder() {}

    @Override
    public FetchScanTasksResponse build() {
      return new FetchScanTasksResponse(planTasks(), fileScanTasks(), deleteFiles(), specsById());
    }
  }
}
