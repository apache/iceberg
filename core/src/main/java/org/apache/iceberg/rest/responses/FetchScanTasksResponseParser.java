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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class FetchScanTasksResponseParser {
  private static final String PLAN_TASKS = "plan-tasks";

  private FetchScanTasksResponseParser() {}

  public static String toJson(FetchScanTasksResponse response) {
    return toJson(response, false);
  }

  public static String toJson(FetchScanTasksResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(FetchScanTasksResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(response != null, "Invalid fetch scan tasks response: null");
    Preconditions.checkArgument(
        response.specsById() != null
            || (response.fileScanTasks() == null || response.fileScanTasks().isEmpty()),
        "Cannot serialize fileScanTasks in fetchScanTasksResponse without specsById");
    gen.writeStartObject();
    if (response.planTasks() != null) {
      JsonUtil.writeStringArray(PLAN_TASKS, response.planTasks(), gen);
    }

    TableScanResponseParser.serializeScanTasks(
        response.fileScanTasks(), response.deleteFiles(), response.specsById(), gen);
    gen.writeEndObject();
  }

  public static FetchScanTasksResponse fromJson(String json) {
    Preconditions.checkArgument(json != null, "Invalid fetch scan tasks response: null");
    return JsonUtil.parse(json, FetchScanTasksResponseParser::fromJson);
  }

  public static FetchScanTasksResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(), "Invalid fetch scan tasks response: null or empty");
    List<String> planTasks = JsonUtil.getStringListOrNull(PLAN_TASKS, json);
    List<DeleteFile> deleteFiles = TableScanResponseParser.parseDeleteFiles(json);
    List<FileScanTask> fileScanTasks =
        TableScanResponseParser.parseFileScanTasks(json, deleteFiles);
    return FetchScanTasksResponse.builder()
        .withPlanTasks(planTasks)
        .withFileScanTasks(fileScanTasks)
        .withDeleteFiles(deleteFiles)
        .build();
  }
}
