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

import org.apache.iceberg.flink.source.eventtimeextractor.EventTimeExtractorRecordEmitter;
import org.apache.iceberg.flink.source.eventtimeextractor.IcebergEventTimeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides implementations of {@link SerializableRecordEmitter} which could be used for emitting
 * records from an Iceberg split. These are used by the {@link IcebergSourceReader}
 */
public class RecordEmitters {
  private static final Logger LOG = LoggerFactory.getLogger(RecordEmitters.class);

  private RecordEmitters() {}

  public static <T> SerializableRecordEmitter<T> emitter() {
    return (element, output, split) -> {
      output.collect(element.record());
      split.updatePosition(element.fileOffset(), element.recordOffset());
    };
  }

  public static <T> SerializableRecordEmitter<T> emitter(IcebergEventTimeExtractor extractor) {
    return new EventTimeExtractorRecordEmitter(extractor);
  }
}
