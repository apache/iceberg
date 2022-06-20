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
package org.apache.iceberg.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.util.JsonUtil;

class ViewHistoryEntryParser {

  // visible for testing
  static final String VERSION_ID = "version-id";
  static final String TIMESTAMP_MS = "timestamp-ms";

  static void toJson(ViewHistoryEntry entry, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(TIMESTAMP_MS, entry.timestampMillis());
    generator.writeNumberField(VERSION_ID, entry.versionId());
    generator.writeEndObject();
  }

  static ViewHistoryEntry fromJson(JsonNode node) {
    return BaseViewHistoryEntry.of(
        JsonUtil.getLong(TIMESTAMP_MS, node), JsonUtil.getInt(VERSION_ID, node));
  }

  private ViewHistoryEntryParser() {}
}
