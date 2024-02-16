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
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PartitionUtil;

class ManifestEntriesTableTaskParser {
  private static final String FILE_IO = "file-io";
  private static final String SPECS = "partition-specs";
  private static final String SCHEMA = "schema";
  private static final String PROJECTION = "projection";
  private static final String RESIDUAL = "residual-filter";
  private static final String MANIFEST = "manifest-file";

  private ManifestEntriesTableTaskParser() {}

  static void toJson(BaseEntriesTable.ManifestReadTask task, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(task != null, "Invalid manifest task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeFieldName(SCHEMA);
    SchemaParser.toJson(task.dataTableSchema(), generator);

    generator.writeFieldName(FILE_IO);
    FileIOParser.toJson(task.io(), generator);

    generator.writeArrayFieldStart(SPECS);
    for (PartitionSpec spec : task.specsById().values()) {
      PartitionSpecParser.toJson(spec, generator);
    }

    generator.writeEndArray();

    generator.writeFieldName(MANIFEST);
    ManifestFileParser.toJson(task.manifest(), generator);

    generator.writeFieldName(PROJECTION);
    SchemaParser.toJson(task.projection(), generator);

    generator.writeFieldName(RESIDUAL);
    ExpressionParser.toJson(task.residual(), generator);
  }

  static BaseEntriesTable.ManifestReadTask fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for manifest task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for manifest task: non-object (%s)", jsonNode);

    Schema dataTableSchema = SchemaParser.fromJson(JsonUtil.get(SCHEMA, jsonNode));
    FileIO fileIO = FileIOParser.fromJson(JsonUtil.get(FILE_IO, jsonNode), null);

    JsonNode specsArray = jsonNode.get(SPECS);
    Preconditions.checkArgument(
        specsArray.isArray(), "Invalid JSON node for partition specs: non-array (%s)", specsArray);
    ImmutableList.Builder<PartitionSpec> specsBuilder = ImmutableList.builder();
    for (JsonNode specNode : specsArray) {
      PartitionSpec spec = PartitionSpecParser.fromJson(dataTableSchema, specNode);
      specsBuilder.add(spec);
    }

    Map<Integer, PartitionSpec> specsById = PartitionUtil.indexSpecs(specsBuilder.build());

    ManifestFile manifestFile = ManifestFileParser.fromJson(JsonUtil.get(MANIFEST, jsonNode));
    Schema projection = SchemaParser.fromJson(JsonUtil.get(PROJECTION, jsonNode));
    Expression residualFilter = ExpressionParser.fromJson(JsonUtil.get(RESIDUAL, jsonNode));

    return new BaseEntriesTable.ManifestReadTask(
        dataTableSchema, fileIO, specsById, manifestFile, projection, residualFilter);
  }
}
