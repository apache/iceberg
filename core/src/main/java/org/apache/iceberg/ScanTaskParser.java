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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.JsonUtil;

public class ScanTaskParser {
  private static final String TASK_TYPE = "task-type";

  private enum TaskType {
    FILE_SCAN_TASK("file-scan-task"),
    DATA_TASK("data-task"),
    FILES_TABLE_TASK("files-table-task"),
    ALL_MANIFESTS_TABLE_TASK("all-manifests-table-task"),
    MANIFEST_ENTRIES_TABLE_TASK("manifest-entries-task");

    private final String value;

    TaskType(String value) {
      this.value = value;
    }

    public static TaskType fromTypeName(String value) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(value), "Invalid task type name: null or empty");
      if (FILE_SCAN_TASK.typeName().equalsIgnoreCase(value)) {
        return FILE_SCAN_TASK;
      } else if (DATA_TASK.typeName().equalsIgnoreCase(value)) {
        return DATA_TASK;
      } else if (FILES_TABLE_TASK.typeName().equalsIgnoreCase(value)) {
        return FILES_TABLE_TASK;
      } else if (ALL_MANIFESTS_TABLE_TASK.typeName().equalsIgnoreCase(value)) {
        return ALL_MANIFESTS_TABLE_TASK;
      } else if (MANIFEST_ENTRIES_TABLE_TASK.typeName().equalsIgnoreCase(value)) {
        return MANIFEST_ENTRIES_TABLE_TASK;
      } else {
        throw new IllegalArgumentException("Unknown task type: " + value);
      }
    }

    public String typeName() {
      return value;
    }
  }

  private ScanTaskParser() {}

  public static String toJson(FileScanTask fileScanTask) {
    Preconditions.checkArgument(fileScanTask != null, "Invalid scan task: null");
    return JsonUtil.generate(generator -> toJson(fileScanTask, generator), false);
  }

  public static FileScanTask fromJson(String json, boolean caseSensitive) {
    Preconditions.checkArgument(json != null, "Invalid JSON string for scan task: null");
    return JsonUtil.parse(json, node -> fromJson(node, caseSensitive));
  }

  private static void toJson(FileScanTask fileScanTask, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();

    if (fileScanTask instanceof StaticDataTask) {
      generator.writeStringField(TASK_TYPE, TaskType.DATA_TASK.typeName());
      DataTaskParser.toJson((StaticDataTask) fileScanTask, generator);
    } else if (fileScanTask instanceof BaseFilesTable.ManifestReadTask) {
      generator.writeStringField(TASK_TYPE, TaskType.FILES_TABLE_TASK.typeName());
      FilesTableTaskParser.toJson((BaseFilesTable.ManifestReadTask) fileScanTask, generator);
    } else if (fileScanTask instanceof AllManifestsTable.ManifestListReadTask) {
      generator.writeStringField(TASK_TYPE, TaskType.ALL_MANIFESTS_TABLE_TASK.typeName());
      AllManifestsTableTaskParser.toJson(
          (AllManifestsTable.ManifestListReadTask) fileScanTask, generator);
    } else if (fileScanTask instanceof BaseEntriesTable.ManifestReadTask) {
      generator.writeStringField(TASK_TYPE, TaskType.MANIFEST_ENTRIES_TABLE_TASK.typeName());
      ManifestEntriesTableTaskParser.toJson(
          (BaseEntriesTable.ManifestReadTask) fileScanTask, generator);
    } else if (fileScanTask instanceof BaseFileScanTask
        || fileScanTask instanceof BaseFileScanTask.SplitScanTask) {
      generator.writeStringField(TASK_TYPE, TaskType.FILE_SCAN_TASK.typeName());
      FileScanTaskParser.toJson(fileScanTask, generator);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported task type: " + fileScanTask.getClass().getCanonicalName());
    }

    generator.writeEndObject();
  }

  private static FileScanTask fromJson(JsonNode jsonNode, boolean caseSensitive) {
    TaskType taskType = TaskType.FILE_SCAN_TASK;
    String taskTypeStr = JsonUtil.getStringOrNull(TASK_TYPE, jsonNode);
    if (null != taskTypeStr) {
      taskType = TaskType.fromTypeName(taskTypeStr);
    }

    switch (taskType) {
      case FILE_SCAN_TASK:
        return FileScanTaskParser.fromJson(jsonNode, caseSensitive);
      case DATA_TASK:
        return DataTaskParser.fromJson(jsonNode);
      case FILES_TABLE_TASK:
        return FilesTableTaskParser.fromJson(jsonNode);
      case ALL_MANIFESTS_TABLE_TASK:
        return AllManifestsTableTaskParser.fromJson(jsonNode);
      case MANIFEST_ENTRIES_TABLE_TASK:
        return ManifestEntriesTableTaskParser.fromJson(jsonNode);
      default:
        throw new UnsupportedOperationException("Unsupported task type: " + taskType.typeName());
    }
  }
}
