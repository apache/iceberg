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

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An UnboundGenericDeleteFile is a GenericDeleteFile which keeps track of the raw partition value
 * represented as JSON
 */
class UnboundGenericDeleteFile extends GenericDeleteFile {
  private JsonNode rawPartitionValue;

  UnboundGenericDeleteFile(
      int specId,
      FileContent content,
      String filePath,
      FileFormat format,
      JsonNode rawPartitionValue,
      long fileSizeInBytes,
      Metrics metrics,
      int[] equalityFieldIds,
      Integer sortOrderId,
      List<Long> splitOffsets,
      ByteBuffer keyMetadata) {
    super(
        specId,
        content,
        filePath,
        format,
        null,
        fileSizeInBytes,
        metrics,
        equalityFieldIds,
        sortOrderId,
        splitOffsets,
        keyMetadata,
        null,
        null,
        null);
    this.rawPartitionValue = rawPartitionValue;
  }

  GenericDeleteFile bindToSpec(PartitionSpec spec) {
    return new GenericDeleteFile(
        specId(),
        content(),
        path().toString(),
        format(),
        ContentFileParser.partitionDataFromRawValue(rawPartitionValue, spec),
        fileSizeInBytes(),
        new Metrics(
            recordCount(),
            columnSizes(),
            valueCounts(),
            nullValueCounts(),
            nanValueCounts(),
            lowerBounds(),
            upperBounds()),
        equalityFieldIds().stream().mapToInt(Integer::intValue).toArray(),
        sortOrderId(),
        splitOffsets(),
        keyMetadata(),
        null,
        null,
        null);
  }
}
