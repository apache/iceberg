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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
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

  private final boolean caseSensitive;
  private final LoadingCache<PartitionSpec, ContentFileParser> contentFileParsersBySpec;

  public FileScanTaskParser(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    this.contentFileParsersBySpec =
        Caffeine.newBuilder().weakKeys().build(spec -> new ContentFileParser(spec));
  }

  public String toJson(FileScanTask fileScanTask) {
    try (StringWriter writer = new StringWriter()) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(fileScanTask, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write json for: " + fileScanTask, e);
    }
  }

  void toJson(FileScanTask fileScanTask, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeFieldName(SCHEMA);
    SchemaParser.toJson(fileScanTask.schema(), generator);

    generator.writeFieldName(SPEC);
    PartitionSpecParser.toJson(fileScanTask.spec(), generator);

    ContentFileParser contentFileParser = contentFileParsersBySpec.get(fileScanTask.spec());

    if (fileScanTask.file() != null) {
      generator.writeFieldName(DATA_FILE);
      contentFileParser.toJson(fileScanTask.file(), generator);
    }

    if (fileScanTask.deletes() != null) {
      generator.writeArrayFieldStart(DELETE_FILES);
      for (DeleteFile deleteFile : fileScanTask.deletes()) {
        contentFileParser.toJson(deleteFile, generator);
      }
      generator.writeEndArray();
    }

    if (fileScanTask.residual() != null) {
      generator.writeFieldName(RESIDUAL);
      ExpressionParser.toJson(fileScanTask.residual(), generator);
    }

    generator.writeEndObject();
  }

  public FileScanTask fromJson(String json) {
    return JsonUtil.parse(json, this::fromJson);
  }

  FileScanTask fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode.isObject(), "Cannot parse file scan task from a non-object: %s", jsonNode);

    JsonNode schemaNode = jsonNode.get(SCHEMA);
    Schema schema = SchemaParser.fromJson(schemaNode);
    String schemaString = SchemaParser.toJson(schema);

    JsonNode specNode = jsonNode.get(SPEC);
    PartitionSpec spec = PartitionSpecParser.fromJson(schema, specNode);
    String specString = PartitionSpecParser.toJson(spec);

    ContentFileParser contentFileParser = contentFileParsersBySpec.get(spec);

    DataFile dataFile = null;
    if (jsonNode.has(DATA_FILE)) {
      dataFile = (DataFile) contentFileParser.fromJson(jsonNode.get(DATA_FILE));
    }

    DeleteFile[] deleteFiles = null;
    if (jsonNode.has(DELETE_FILES)) {
      JsonNode deletesArray = jsonNode.get(DELETE_FILES);
      Preconditions.checkArgument(
          deletesArray.isArray(), "Cannot parse delete files from a non-array: %s", deletesArray);
      // parse the schema array
      ImmutableList.Builder<DeleteFile> builder = ImmutableList.builder();
      for (JsonNode deleteFileNode : deletesArray) {
        DeleteFile deleteFile = (DeleteFile) contentFileParser.fromJson(deleteFileNode);
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
