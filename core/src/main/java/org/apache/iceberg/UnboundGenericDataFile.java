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
 * An UnboundGenericDataFile is a GenericDataFile which keeps track of the raw partition value
 * represented as JSON
 */
class UnboundGenericDataFile extends GenericDataFile {
  private final JsonNode rawPartitionValue;

  UnboundGenericDataFile(
      int specId,
      String filePath,
      FileFormat format,
      JsonNode rawPartitionValue,
      long fileSizeInBytes,
      Metrics metrics,
      ByteBuffer keyMetadata,
      List<Long> splitOffsets,
      Integer sortOrderId) {
    super(
        specId,
        filePath,
        format,
        null,
        fileSizeInBytes,
        metrics,
        keyMetadata,
        splitOffsets,
        sortOrderId,
        -1L); // track row-lineage
    this.rawPartitionValue = rawPartitionValue;
  }

  GenericDataFile bindToSpec(PartitionSpec spec) {
    return new GenericDataFile(
        specId(),
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
        keyMetadata(),
        splitOffsets(),
        sortOrderId(),
        -1L);
  }
}
