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
package org.apache.iceberg.flink.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emitter which emits the watermarks, records and updates the split position.
 *
 * <p>The Emitter emits watermarks at the beginning of every split provided by the {@link
 * SplitWatermarkExtractor}.
 */
class WatermarkExtractorRecordEmitter<T> implements SerializableRecordEmitter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkExtractorRecordEmitter.class);
  private final SplitWatermarkExtractor timeExtractor;
  private String lastSplitId = null;
  private long watermark;

  WatermarkExtractorRecordEmitter(SplitWatermarkExtractor timeExtractor) {
    this.timeExtractor = timeExtractor;
  }

  @Override
  public void emitRecord(
      RecordAndPosition<T> element, SourceOutput<T> output, IcebergSourceSplit split) {
    if (!split.splitId().equals(lastSplitId)) {
      long newWatermark = timeExtractor.extractWatermark(split);
      if (newWatermark < watermark) {
        LOG.info(
            "Received a new split with lower watermark. Previous watermark = {}, current watermark = {}, previous split = {}, current split = {}",
            watermark,
            newWatermark,
            lastSplitId,
            split.splitId());
      } else {
        watermark = newWatermark;
        output.emitWatermark(new Watermark(watermark));
        LOG.debug("Watermark = {} emitted based on split = {}", watermark, lastSplitId);
      }

      lastSplitId = split.splitId();
    }

    output.collect(element.record());
    split.updatePosition(element.fileOffset(), element.recordOffset());
  }
}
