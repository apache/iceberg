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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

public class GetScanTasksResponse implements RESTResponse {

  private List<FileScanTask> fileScanTasks;

  private String next;

  public GetScanTasksResponse() {}

  private GetScanTasksResponse(List<FileScanTask> fileScanTasks, String nextToken) {
    this.fileScanTasks = fileScanTasks;
    this.next = nextToken;
  }

  public List<FileScanTask> fileScanTasks() {
    return fileScanTasks;
  }

  public String nextToken() {
    return next;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(fileScanTasks != null, "Invalid fileScanTasks: null");
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fileScanTasks", fileScanTasks)
        .add("next", next)
        .toString();
  }

  public static GetScanTasksResponse.Builder builder() {
    return new GetScanTasksResponse.Builder();
  }

  public static class Builder {

    private List<FileScanTask> fileScanTasks;
    private String next;

    private Builder() {}

    public GetScanTasksResponse.Builder withFileScanTasks(List<FileScanTask> fileScanTaskList) {
      Preconditions.checkNotNull(fileScanTaskList, "Invalid fileScanTasks: null");
      this.fileScanTasks = fileScanTaskList;
      return this;
    }

    public GetScanTasksResponse.Builder withNextToken(String nextToken) {
      this.next = nextToken;
      return this;
    }

    public GetScanTasksResponse build() {
      return new GetScanTasksResponse(fileScanTasks, next);
    }
  }
}
