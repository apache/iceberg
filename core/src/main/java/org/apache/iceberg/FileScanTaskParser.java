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
import java.io.StringWriter;
import java.io.UncheckedIOException;
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
  private static final String DELETE_FILES = "delete-files";
  private static final String RESIDUAL = "residual-filter";

  private FileScanTaskParser() {}

  public static String toJson(FileScanTask fileScanTask) {
    Preconditions.checkArgument(fileScanTask != null, "File scan task cannot be null");
    try (StringWriter writer = new StringWriter()) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(fileScanTask, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write json for: " + fileScanTask, e);
    }
  }

  private static void toJson(FileScanTask fileScanTask, JsonGenerator generator)
      throws IOException {
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
    Preconditions.checkArgument(json != null, "Cannot parse file scan task from null JSON string");
    try {
      JsonNode jsonNode = JsonUtil.mapper().readValue(json, JsonNode.class);
      return fromJsonNode(jsonNode, caseSensitive);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static FileScanTask fromJsonNode(JsonNode jsonNode, boolean caseSensitive) {
    Preconditions.checkArgument(
        jsonNode.isObject(), "Cannot parse file scan task from a non-object: %s", jsonNode);

    JsonNode schemaNode = jsonNode.get(SCHEMA);
    Schema schema = SchemaParser.fromJson(schemaNode);
    String schemaString = SchemaParser.toJson(schema);

    JsonNode specNode = jsonNode.get(SPEC);
    PartitionSpec spec = PartitionSpecParser.fromJson(schema, specNode);
    String specString = PartitionSpecParser.toJson(spec);

    DataFile dataFile = null;
    if (jsonNode.has(DATA_FILE)) {
      dataFile = (DataFile) ContentFileParser.fromJson(jsonNode.get(DATA_FILE), spec);
    }

    DeleteFile[] deleteFiles = null;
    if (jsonNode.has(DELETE_FILES)) {
      JsonNode deletesArray = jsonNode.get(DELETE_FILES);
      Preconditions.checkArgument(
          deletesArray.isArray(), "Cannot parse delete files from a non-array: %s", deletesArray);
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
    return new BaseFileScanTask(dataFile, deleteFiles, schemaString, specString, residualEvaluator);
  }
}
