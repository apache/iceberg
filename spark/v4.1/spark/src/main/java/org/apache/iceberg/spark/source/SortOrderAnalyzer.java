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
package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SortOrderAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(SortOrderAnalyzer.class);

  private SortOrderAnalyzer() {}

  static int reportablePrefixLength(Table table, Schema projection) {
    SortOrder sortOrder = table.sortOrder();
    if (sortOrder.isUnsorted()) {
      LOG.debug("Cannot report ordering for table {}: table is unsorted", table.name());
      return 0;
    }

    if (!hasMergeableSortKeys(sortOrder, table.schema(), table.name())) {
      return 0;
    }

    int prefixLength = countReportableFields(sortOrder, projection);
    if (prefixLength == 0) {
      LOG.debug(
          "Cannot report ordering for table {}: first sort key is not projected or not identity",
          table.name());
    }

    return prefixLength;
  }

  static boolean canMerge(List<? extends ScanTask> tasks, int sortOrderId) {
    if (tasks.isEmpty()) {
      LOG.debug("Cannot report ordering: no tasks");
      return false;
    }

    for (ScanTask task : tasks) {
      if (!canMerge(task, sortOrderId)) {
        return false;
      }
    }

    return true;
  }

  static boolean withinFileLimit(List<? extends ScanTaskGroup<?>> taskGroups, int maxFilesPerTask) {
    for (ScanTaskGroup<?> taskGroup : taskGroups) {
      if (taskGroup.tasks().size() > maxFilesPerTask) {
        LOG.debug(
            "Cannot report ordering: a task group has {} tasks, more than the limit of {}",
            taskGroup.tasks().size(),
            maxFilesPerTask);
        return false;
      }
    }

    return true;
  }

  private static boolean hasMergeableSortKeys(
      SortOrder sortOrder, Schema schema, String tableName) {
    List<SortField> sortFields = sortOrder.fields();
    for (int i = 0; i < sortFields.size(); i++) {
      SortField sortField = sortFields.get(i);
      if (!TypeUtil.ancestorFields(schema, sortField.sourceId()).isEmpty()) {
        LOG.debug("Cannot report ordering for table {}: nested sort key", tableName);
        return false;
      }

      Types.NestedField field = schema.findField(sortField.sourceId());
      if (field == null) {
        LOG.debug("Cannot report ordering for table {}: sort key not in schema", tableName);
        return false;
      }

      Type.TypeID resultType = sortField.transform().getResultType(field.type()).typeId();
      if (resultType == Type.TypeID.UUID) {
        LOG.debug("Cannot report ordering for table {}: UUID sort key", tableName);
        return false;
      }

      boolean floatingPoint = resultType == Type.TypeID.FLOAT || resultType == Type.TypeID.DOUBLE;
      if (floatingPoint && i < sortFields.size() - 1) {
        LOG.debug(
            "Cannot report ordering for table {}: floating point sort key is not last", tableName);
        return false;
      }
    }

    return true;
  }

  // Spark can only use leading sort fields that are projected identity transforms
  private static int countReportableFields(SortOrder sortOrder, Schema projection) {
    int count = 0;
    for (SortField sortField : sortOrder.fields()) {
      if (!sortField.transform().isIdentity()
          || projection.findField(sortField.sourceId()) == null) {
        break;
      }

      count += 1;
    }

    return count;
  }

  private static boolean canMerge(ScanTask task, int sortOrderId) {
    if (!(task instanceof FileScanTask fileTask)) {
      LOG.debug("Cannot report ordering: unsupported task type {}", task.getClass().getName());
      return false;
    }

    Integer fileSortOrderId = fileTask.file().sortOrderId();
    if (fileSortOrderId == null || fileSortOrderId != sortOrderId) {
      LOG.debug(
          "Cannot report ordering: file {} has sort order {}, expected {}",
          fileTask.file().location(),
          fileSortOrderId,
          sortOrderId);
      return false;
    }

    for (DeleteFile deleteFile : fileTask.deletes()) {
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        LOG.debug(
            "Cannot report ordering: file {} has equality deletes", fileTask.file().location());
        return false;
      }
    }

    return true;
  }
}
