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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTContentFileParser;
import org.apache.iceberg.rest.RESTFileScanTaskParser;
import org.apache.iceberg.util.JsonUtil;

public class FetchScanTasksResponseParser {
  private static final String PLAN_TASKS = "plan-tasks";
  private static final String FILE_SCAN_TASKS = "file-scan-tasks";
  private static final String DELETE_FILES = "delete-files";

  private FetchScanTasksResponseParser() {}

  public static String toJson(FetchScanTasksResponse response) {
    return toJson(response, false);
  }

  public static String toJson(FetchScanTasksResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(FetchScanTasksResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid response: fetchScanTasksResponse null");
    gen.writeStartObject();
    if (response.planTasks() != null) {
      gen.writeArrayFieldStart(PLAN_TASKS);
      for (String planTask : response.planTasks()) {
        gen.writeString(planTask);
      }
      gen.writeEndArray();
    }

    List<DeleteFile> deleteFiles = null;
    if (response.deleteFiles() != null) {
      deleteFiles = response.deleteFiles();
      gen.writeArrayFieldStart(DELETE_FILES);
      for (DeleteFile deleteFile : deleteFiles) {
        RESTContentFileParser.toJson(deleteFile);
      }
      gen.writeEndArray();
    }

    if (response.fileScanTasks() != null) {
      gen.writeArrayFieldStart(FILE_SCAN_TASKS);
      for (FileScanTask fileScanTask : response.fileScanTasks()) {
        RESTFileScanTaskParser.toJson(fileScanTask, deleteFiles, gen);
      }
      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static FetchScanTasksResponse fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse fetchScanTasks response from null");
    return JsonUtil.parse(json, FetchScanTasksResponseParser::fromJson);
  }

  public static FetchScanTasksResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(),
        "Cannot parse fetchScanTasks response from empty or null object");

    List<String> planTasks = JsonUtil.getStringListOrNull(PLAN_TASKS, json);

    List<DeleteFile> allDeleteFiles = null;
    if (json.has(DELETE_FILES)) {
      JsonNode deletesArray = json.get(DELETE_FILES);
      ImmutableList.Builder<DeleteFile> deleteFilesBuilder = ImmutableList.builder();
      for (JsonNode deleteFileNode : deletesArray) {
        DeleteFile deleteFile = (DeleteFile) RESTContentFileParser.fromJson(deleteFileNode);
        deleteFilesBuilder.add(deleteFile);
      }
      allDeleteFiles = deleteFilesBuilder.build();
    }

    List<FileScanTask> fileScanTasks = null;
    if (json.has(FILE_SCAN_TASKS)) {
      JsonNode fileScanTasksArray = json.get(FILE_SCAN_TASKS);
      ImmutableList.Builder<FileScanTask> fileScanTaskBuilder = ImmutableList.builder();
      for (JsonNode fileScanTaskNode : fileScanTasksArray) {
        // TODO we dont have caseSensitive flag at serial/deserialize time
        FileScanTask fileScanTask =
            (FileScanTask) RESTFileScanTaskParser.fromJson(fileScanTaskNode, allDeleteFiles);
        fileScanTaskBuilder.add(fileScanTask);
      }
      fileScanTasks = fileScanTaskBuilder.build();
    }

    return new FetchScanTasksResponse.Builder()
        .withPlanTasks(planTasks)
        .withFileScanTasks(fileScanTasks)
        .withDeleteFiles(allDeleteFiles)
        .build();
  }
}
