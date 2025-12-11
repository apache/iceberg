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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class RESTFileScanTaskParser {
  private static final String DATA_FILE = "data-file";
  private static final String DELETE_FILE_REFERENCES = "delete-file-references";
  private static final String RESIDUAL_FILTER = "residual-filter";

  private RESTFileScanTaskParser() {}

  public static void toJson(
      FileScanTask fileScanTask,
      Set<Integer> deleteFileReferences,
      PartitionSpec partitionSpec,
      JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(fileScanTask != null, "Invalid file scan task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();
    generator.writeFieldName(DATA_FILE);
    ContentFileParser.toJson(fileScanTask.file(), partitionSpec, generator);
    if (deleteFileReferences != null && !deleteFileReferences.isEmpty()) {
      JsonUtil.writeIntegerArray(DELETE_FILE_REFERENCES, deleteFileReferences, generator);
    }

    if (fileScanTask.residual() != null) {
      generator.writeFieldName(RESIDUAL_FILTER);
      ExpressionParser.toJson(fileScanTask.residual(), generator);
    }

    generator.writeEndObject();
  }

  public static FileScanTask fromJson(
      JsonNode jsonNode,
      List<DeleteFile> allDeleteFiles,
      Map<Integer, PartitionSpec> specsById,
      boolean isCaseSensitive) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for file scan task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for file scan task: non-object (%s)", jsonNode);

    DataFile dataFile =
        (DataFile) ContentFileParser.fromJson(JsonUtil.get(DATA_FILE, jsonNode), specsById);
    int specId = dataFile.specId();

    DeleteFile[] deleteFiles = null;
    if (jsonNode.has(DELETE_FILE_REFERENCES)) {
      List<Integer> indices = JsonUtil.getIntegerList(DELETE_FILE_REFERENCES, jsonNode);
      Preconditions.checkArgument(
          indices.isEmpty() || Collections.max(indices) < allDeleteFiles.size(),
          "Invalid delete file references: %s, expected indices < %s",
          indices,
          allDeleteFiles.size());
      deleteFiles = indices.stream().map(allDeleteFiles::get).toArray(DeleteFile[]::new);
    }

    Expression filter = null;
    if (jsonNode.has(RESIDUAL_FILTER)) {
      filter = ExpressionParser.fromJson(jsonNode.get(RESIDUAL_FILTER));
    }

    String schemaString = SchemaParser.toJson(specsById.get(specId).schema());
    String specString = PartitionSpecParser.toJson(specsById.get(specId));
    ResidualEvaluator boundResidual =
        ResidualEvaluator.of(specsById.get(specId), filter, isCaseSensitive);

    return new BaseFileScanTask(dataFile, deleteFiles, schemaString, specString, boundResidual);
  }
}
