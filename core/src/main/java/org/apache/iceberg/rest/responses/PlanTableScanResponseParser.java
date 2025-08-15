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
import java.util.Map;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.TableScanResponseParser;
import org.apache.iceberg.util.JsonUtil;

public class PlanTableScanResponseParser {
  private static final String PLAN_STATUS = "plan-status";
  private static final String PLAN_ID = "plan-id";
  private static final String PLAN_TASKS = "plan-tasks";

  private PlanTableScanResponseParser() {}

  public static String toJson(PlanTableScanResponse response) {
    return toJson(response, false);
  }

  public static String toJson(PlanTableScanResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(PlanTableScanResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid response: planTableScanResponse null");
    Preconditions.checkArgument(
        response.planStatus() != null, "Invalid response: status can not be null");
    Preconditions.checkArgument(
        response.specsById() != null, "Cannot serialize planTableScanResponse without specsById");

    gen.writeStartObject();
    gen.writeStringField(PLAN_STATUS, response.planStatus().status());

    if (response.planId() != null) {
      gen.writeStringField(PLAN_ID, response.planId());
    }
    if (response.planTasks() != null) {
      JsonUtil.writeStringArray(PLAN_TASKS, response.planTasks(), gen);
    }

    TableScanResponseParser.serializeScanTasks(
        response.fileScanTasks(), response.deleteFiles(), response.specsById(), gen);

    gen.writeEndObject();
  }

  @VisibleForTesting
  static PlanTableScanResponse fromJson(
      String json, Map<Integer, PartitionSpec> specsById, boolean caseSensitive) {
    Preconditions.checkArgument(
        json != null, "Cannot parse planTableScan response from empty or null object");
    return JsonUtil.parse(
        json,
        node -> {
          return PlanTableScanResponseParser.fromJson(node, specsById, caseSensitive);
        });
  }

  public static PlanTableScanResponse fromJson(
      JsonNode json, Map<Integer, PartitionSpec> specsById, boolean caseSensitive) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(),
        "Cannot parse planTableScan response from empty or null object");

    PlanStatus planStatus = PlanStatus.fromName(JsonUtil.getString(PLAN_STATUS, json));
    String planId = JsonUtil.getStringOrNull(PLAN_ID, json);
    List<String> planTasks = JsonUtil.getStringListOrNull(PLAN_TASKS, json);
    List<DeleteFile> deleteFiles = TableScanResponseParser.parseDeleteFiles(json, specsById);
    List<FileScanTask> fileScanTasks =
        TableScanResponseParser.parseFileScanTasks(json, deleteFiles, specsById, caseSensitive);

    return PlanTableScanResponse.builder()
        .withPlanId(planId)
        .withPlanStatus(planStatus)
        .withPlanTasks(planTasks)
        .withFileScanTasks(fileScanTasks)
        .withDeleteFiles(deleteFiles)
        .build();
  }
}
