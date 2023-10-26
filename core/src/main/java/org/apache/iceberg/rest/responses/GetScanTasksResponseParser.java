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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FileScanTaskParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class GetScanTasksResponseParser {
  static final String FILE_SCAN_TASKS = "file-scan-tasks";
  static final String NEXT_TOKEN = "next";

  private GetScanTasksResponseParser() {}

  public static String toJson(GetScanTasksResponse response) {
    return toJson(response, false);
  }

  public static String toJson(GetScanTasksResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(GetScanTasksResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid get scan task response : null");

    gen.writeStartObject();
    gen.writeArrayFieldStart(FILE_SCAN_TASKS);
    for (FileScanTask fileScanTask : response.fileScanTasks()) {
      FileScanTaskParser.toJson(fileScanTask);
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }

  public static GetScanTasksResponse fromJson(String json) {
    Preconditions.checkArgument(
        json != null, "Cannot parse get scan tasks metadata from null string");
    return JsonUtil.parse(json, GetScanTasksResponseParser::fromJson);
  }

  public static GetScanTasksResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse get scan tasks from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse get scan tasks metadata from non-object: %s", json);

    JsonNode fileScanTasksJson = JsonUtil.get("file-scan-tasks", json);
    List<FileScanTask> fileScanTaskList = Lists.newArrayList();
    for (JsonNode fileScanTaskNode : fileScanTasksJson) {
      fileScanTaskList.add(FileScanTaskParser.fromJson(fileScanTaskNode.toString(), false));
    }
    String nextToken = null;
    if (json.get(NEXT_TOKEN) != null) {
      nextToken = String.valueOf(JsonUtil.getString(NEXT_TOKEN, json));
    }
    return GetScanTasksResponse.builder()
        .withFileScanTasks(fileScanTaskList)
        .withNextToken(nextToken)
        .build();
  }
}
