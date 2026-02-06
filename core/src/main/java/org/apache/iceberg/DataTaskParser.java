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
import org.apache.iceberg.util.JsonUtil;

class DataTaskParser {
  private static final String SCHEMA = "schema";
  private static final String PROJECTED_SCHEMA = "projection";
  private static final String METADATA_FILE = "metadata-file";
  private static final String ROWS = "rows";

  private DataTaskParser() {}

  static void toJson(StaticDataTask dataTask, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(dataTask != null, "Invalid data task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeFieldName(SCHEMA);
    SchemaParser.toJson(dataTask.schema(), generator);

    generator.writeFieldName(PROJECTED_SCHEMA);
    SchemaParser.toJson(dataTask.projectedSchema(), generator);

    generator.writeFieldName(METADATA_FILE);
    ContentFileParser.toJson(dataTask.metadataFile(), PartitionSpec.unpartitioned(), generator);

    Preconditions.checkArgument(dataTask.tableRows() != null, "Invalid data task: null table rows");
    generator.writeArrayFieldStart(ROWS);
    for (StructLike row : dataTask.tableRows()) {
      SingleValueParser.toJson(dataTask.schema().asStruct(), row, generator);
    }

    generator.writeEndArray();
  }

  static StaticDataTask fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for data task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for data task: non-object (%s)", jsonNode);

    Schema schema = SchemaParser.fromJson(JsonUtil.get(SCHEMA, jsonNode));
    Schema projectedSchema = SchemaParser.fromJson(JsonUtil.get(PROJECTED_SCHEMA, jsonNode));
    DataFile metadataFile =
        (DataFile)
            ContentFileParser.fromJson(
                JsonUtil.get(METADATA_FILE, jsonNode), PartitionSpec.unpartitioned());

    JsonNode rowsArray = JsonUtil.get(ROWS, jsonNode);
    Preconditions.checkArgument(
        rowsArray.isArray(), "Invalid JSON node for rows: non-array (%s)", rowsArray);

    StructLike[] rows = new StructLike[rowsArray.size()];
    for (int i = 0; i < rowsArray.size(); ++i) {
      JsonNode rowNode = rowsArray.get(i);
      rows[i] = (StructLike) SingleValueParser.fromJson(schema.asStruct(), rowNode);
    }

    return new StaticDataTask(metadataFile, schema, projectedSchema, rows);
  }
}
