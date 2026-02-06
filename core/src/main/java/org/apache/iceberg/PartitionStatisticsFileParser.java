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
import org.apache.iceberg.util.JsonUtil;

public class PartitionStatisticsFileParser {

  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String STATISTICS_PATH = "statistics-path";
  private static final String FILE_SIZE_IN_BYTES = "file-size-in-bytes";

  private PartitionStatisticsFileParser() {}

  public static String toJson(PartitionStatisticsFile partitionStatisticsFile) {
    return toJson(partitionStatisticsFile, false);
  }

  public static String toJson(PartitionStatisticsFile partitionStatisticsFile, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(partitionStatisticsFile, gen), pretty);
  }

  public static void toJson(PartitionStatisticsFile statisticsFile, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SNAPSHOT_ID, statisticsFile.snapshotId());
    generator.writeStringField(STATISTICS_PATH, statisticsFile.path());
    generator.writeNumberField(FILE_SIZE_IN_BYTES, statisticsFile.fileSizeInBytes());
    generator.writeEndObject();
  }

  static PartitionStatisticsFile fromJson(JsonNode node) {
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    String path = JsonUtil.getString(STATISTICS_PATH, node);
    long fileSizeInBytes = JsonUtil.getLong(FILE_SIZE_IN_BYTES, node);
    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .path(path)
        .fileSizeInBytes(fileSizeInBytes)
        .build();
  }
}
