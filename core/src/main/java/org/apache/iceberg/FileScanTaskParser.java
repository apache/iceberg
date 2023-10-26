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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class FileScanTaskParser {
  private static final String SCHEMA = "schema";
  private static final String SPEC = "spec";
  private static final String DATA_FILE = "data-file";
  private static final String START = "start";
  private static final String LENGTH = "length";
  private static final String DELETE_FILES = "delete-files";
  private static final String RESIDUAL = "residual-filter";

  private FileScanTaskParser() {}

  public static String toJson(FileScanTask fileScanTask) {
    return JsonUtil.generate(
        generator -> FileScanTaskParser.toJson(fileScanTask, generator), false);
  }

  private static void toJson(FileScanTask fileScanTask, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(fileScanTask != null, "Invalid file scan task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");
    generator.writeStartObject();

    generator.writeFieldName(SCHEMA);
    SchemaParser.toJson(fileScanTask.schema(), generator);

    generator.writeFieldName(SPEC);
    PartitionSpec spec = fileScanTask.spec();
    PartitionSpecParser.toJson(spec, generator);

    if (fileScanTask.file() != null) {
      generator.writeFieldName(DATA_FILE);
      ContentFileParser.toJson(fileScanTask.file(), spec, generator);
    }

    generator.writeNumberField(START, fileScanTask.start());
    generator.writeNumberField(LENGTH, fileScanTask.length());

    if (fileScanTask.deletes() != null) {
      generator.writeArrayFieldStart(DELETE_FILES);
      for (DeleteFile deleteFile : fileScanTask.deletes()) {
        ContentFileParser.toJson(deleteFile, spec, generator);
      }
      generator.writeEndArray();
    }

    if (fileScanTask.residual() != null) {
      generator.writeFieldName(RESIDUAL);
      ExpressionParser.toJson(fileScanTask.residual(), generator);
    }

    generator.writeEndObject();
  }

  public static FileScanTask fromJson(String json, boolean caseSensitive) {
    Preconditions.checkArgument(json != null, "Invalid JSON string for file scan task: null");
    return JsonUtil.parse(json, node -> FileScanTaskParser.fromJson(node, caseSensitive));
  }

  private static FileScanTask fromJson(JsonNode jsonNode, boolean caseSensitive) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for file scan task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for file scan task: non-object (%s)", jsonNode);

    Schema schema = SchemaParser.fromJson(JsonUtil.get(SCHEMA, jsonNode));
    String schemaString = SchemaParser.toJson(schema);

    PartitionSpec spec = PartitionSpecParser.fromJson(schema, JsonUtil.get(SPEC, jsonNode));
    String specString = PartitionSpecParser.toJson(spec);

    DataFile dataFile = null;
    if (jsonNode.has(DATA_FILE)) {
      dataFile = (DataFile) ContentFileParser.fromJson(jsonNode.get(DATA_FILE), spec);
    }

    long start = JsonUtil.getLong(START, jsonNode);
    long length = JsonUtil.getLong(LENGTH, jsonNode);

    DeleteFile[] deleteFiles = null;
    if (jsonNode.has(DELETE_FILES)) {
      JsonNode deletesArray = jsonNode.get(DELETE_FILES);
      Preconditions.checkArgument(
          deletesArray.isArray(),
          "Invalid JSON node for delete files: non-array (%s)",
          deletesArray);
      // parse the schema array
      ImmutableList.Builder<DeleteFile> builder = ImmutableList.builder();
      for (JsonNode deleteFileNode : deletesArray) {
        DeleteFile deleteFile = (DeleteFile) ContentFileParser.fromJson(deleteFileNode, spec);
        builder.add(deleteFile);
      }

      deleteFiles = builder.build().toArray(new DeleteFile[0]);
    }

    Expression filter = Expressions.alwaysTrue();
    if (jsonNode.has(RESIDUAL)) {
      filter = ExpressionParser.fromJson(jsonNode.get(RESIDUAL));
    }

    ResidualEvaluator residualEvaluator = ResidualEvaluator.of(spec, filter, caseSensitive);
    BaseFileScanTask baseFileScanTask =
        new BaseFileScanTask(dataFile, deleteFiles, schemaString, specString, residualEvaluator);

    if (start == 0 && length == dataFile.fileSizeInBytes()) {
      // TODO
      // When rest scans are enabled we require this check
      // As fileScanTasks returned from external service are not split
      return baseFileScanTask;
    }
    return new BaseFileScanTask.SplitScanTask(start, length, baseFileScanTask);
  }
}
