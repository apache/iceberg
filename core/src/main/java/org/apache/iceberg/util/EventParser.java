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

package org.apache.iceberg.util;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.ExpressionParser;

public class EventParser {
  private static final String EVENT_TYPE = "event-type";

  private static final String TABLE_NAME = "table-name";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String PROJECTION = "projection";
  private static final String EXPRESSION = "expression";
  private static final String OPERATION = "operation";
  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String SUMMARY = "summary";
  private static final String FROM_SNAPSHOT_ID = "from-snapshot-id";
  private static final String TO_SNAPSHOT_ID = "to-snapshot-id";

  private EventParser() {
  }

  public static String toJson(Object event) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (event instanceof ScanEvent) {
        toJson((ScanEvent) event, generator);
      } else if (event instanceof CreateSnapshotEvent) {
        toJson((CreateSnapshotEvent) event, generator);
      } else if (event instanceof IncrementalScanEvent) {
        toJson((IncrementalScanEvent) event, generator);
      }

      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write json"), e);
    }
  }

  public static void toJson(ScanEvent event, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeFieldName(EVENT_TYPE);
    generator.writeString(event.getClass().getName());
    generator.writeFieldName(TABLE_NAME);
    generator.writeString(event.tableName());
    generator.writeFieldName(SNAPSHOT_ID);
    generator.writeNumber(event.snapshotId());
    generator.writeFieldName(EXPRESSION);
    ExpressionParser.toJson(event.filter(), generator);
    generator.writeFieldName(PROJECTION);
    SchemaParser.toJson(event.projection(), generator);
    generator.writeEndObject();
  }

  public static void toJson(CreateSnapshotEvent event, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeFieldName(EVENT_TYPE);
    generator.writeString(event.getClass().getName());
    generator.writeFieldName(TABLE_NAME);
    generator.writeString(event.tableName());
    generator.writeFieldName(OPERATION);
    generator.writeString(event.operation());
    generator.writeFieldName(SNAPSHOT_ID);
    generator.writeNumber(event.snapshotId());
    generator.writeFieldName(SEQUENCE_NUMBER);
    generator.writeNumber(event.sequenceNumber());
    generator.writeObjectFieldStart(SUMMARY);
    for (Map.Entry<String, String> keyValue : event.summary().entrySet()) {
      generator.writeStringField(keyValue.getKey(), keyValue.getValue());
    }

    generator.writeEndObject();
    generator.writeEndObject();
  }

  public static void toJson(IncrementalScanEvent event, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeFieldName(EVENT_TYPE);
    generator.writeString(event.getClass().getName());
    generator.writeFieldName(TABLE_NAME);
    generator.writeString(event.tableName());
    generator.writeFieldName(FROM_SNAPSHOT_ID);
    generator.writeNumber(event.fromSnapshotId());
    generator.writeFieldName(TO_SNAPSHOT_ID);
    generator.writeNumber(event.toSnapshotId());
    generator.writeFieldName(EXPRESSION);
    ExpressionParser.toJson(event.filter(), generator);
    generator.writeFieldName(PROJECTION);
    SchemaParser.toJson(event.projection(), generator);
    generator.writeEndObject();
  }
}
