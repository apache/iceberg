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
import java.util.List;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class RESTFileScanTaskParser {
  private static final String DATA_FILE = "data-file";
  private static final String DELETE_FILE_REFERENCES = "delete-file-references";
  private static final String RESIDUAL = "residual-filter";

  private RESTFileScanTaskParser() {}

  public static void toJson(
      FileScanTask fileScanTask, List<DeleteFile> deleteFiles, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(fileScanTask != null, "Invalid file scan task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();
    generator.writeFieldName(DATA_FILE);
    RESTContentFileParser.toJson(fileScanTask.file(), generator);

    // TODO revisit this logic
    if (deleteFiles != null) {
      generator.writeArrayFieldStart(DELETE_FILE_REFERENCES);
      for (int delIndex = 0; delIndex < deleteFiles.size(); delIndex++) {
        generator.writeNumber(delIndex);
      }
      generator.writeEndArray();
    }
    if (fileScanTask.residual() != null) {
      generator.writeFieldName(RESIDUAL);
      ExpressionParser.toJson(fileScanTask.residual(), generator);
    }
    generator.writeEndObject();
  }

  public static FileScanTask fromJson(JsonNode jsonNode, List<DeleteFile> allDeleteFiles) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for file scan task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for file scan task: non-object (%s)", jsonNode);

    DataFile dataFile = (DataFile) RESTContentFileParser.fromJson(jsonNode.get(DATA_FILE));

    DeleteFile[] matchedDeleteFiles = null;
    List<Integer> deleteFileReferences = null;
    if (jsonNode.has(DELETE_FILE_REFERENCES)) {
      ImmutableList.Builder deleteFileReferencesBuilder = ImmutableList.builder();
      JsonNode deletesArray = jsonNode.get(DELETE_FILE_REFERENCES);
      for (JsonNode deleteRef : deletesArray) {
        deleteFileReferencesBuilder.add(deleteRef);
      }
      deleteFileReferences = deleteFileReferencesBuilder.build();
    }

    if (deleteFileReferences != null) {
      ImmutableList.Builder matchedDeleteFilesBuilder = ImmutableList.builder();
      for (Integer deleteFileIdx : deleteFileReferences) {
        matchedDeleteFilesBuilder.add(allDeleteFiles.get(deleteFileIdx));
      }
      matchedDeleteFiles = (DeleteFile[]) matchedDeleteFilesBuilder.build().stream().toArray();
    }

    // TODO revisit this in spec
    Expression filter = Expressions.alwaysTrue();
    if (jsonNode.has(RESIDUAL)) {
      filter = ExpressionParser.fromJson(jsonNode.get(RESIDUAL));
    }

    ResidualEvaluator residualEvaluator = ResidualEvaluator.of(filter);

    // TODO at the time of creation we dont have the schemaString and specString so can we avoid
    // setting this
    // will need to refresh before returning closed iterable of tasks, for now put place holder null
    BaseFileScanTask baseFileScanTask =
        new BaseFileScanTask(dataFile, matchedDeleteFiles, null, null, residualEvaluator);
    return baseFileScanTask;
  }
}
