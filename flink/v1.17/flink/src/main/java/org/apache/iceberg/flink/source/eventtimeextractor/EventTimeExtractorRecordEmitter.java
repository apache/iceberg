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
package org.apache.iceberg.flink.source.eventtimeextractor;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.iceberg.flink.source.reader.RecordAndPosition;
import org.apache.iceberg.flink.source.reader.SerializableRecordEmitter;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emitter which emits the record with event time and updates the split position.
 *
 * <p>The Emitter also emits watermarks at the beginning of every split, and sets the event
 * timestamp based on the provided {@link IcebergEventTimeExtractor}.
 */
public final class EventTimeExtractorRecordEmitter<T> implements SerializableRecordEmitter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(EventTimeExtractorRecordEmitter.class);
  private final IcebergEventTimeExtractor timeExtractor;
  private String lastSplit = null;
  private long watermark;

  public EventTimeExtractorRecordEmitter(IcebergEventTimeExtractor timeExtractor) {
    this.timeExtractor = timeExtractor;
  }

  @Override
  public void emitRecord(
      RecordAndPosition<T> element, SourceOutput<T> output, IcebergSourceSplit split) {
    if (!split.splitId().equals(lastSplit)) {
      long newWatermark = timeExtractor.extractWatermark(split);
      if (newWatermark < watermark) {
        LOG.warn(
            "Watermark decreased. PreviousWM {}, currentWM {}, previousSplit {}, currentSplit {}.",
            watermark,
            newWatermark,
            lastSplit,
            split.splitId());
      }
      watermark = newWatermark;
      output.emitWatermark(new Watermark(watermark));
      lastSplit = split.splitId();
    }

    long eventTime = timeExtractor.extractEventTime(element.record());
    if (eventTime <= watermark) {
      LOG.warn(
          "Late event arrived. PreviousWM {}, split {}, eventTime {}, record {}.",
          watermark,
          split,
          eventTime,
          element.record());
    }

    output.collect(element.record(), eventTime);
    split.updatePosition(element.fileOffset(), element.recordOffset());
  }
}
