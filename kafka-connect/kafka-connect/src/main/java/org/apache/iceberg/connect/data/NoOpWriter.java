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
package org.apache.iceberg.connect.data;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Placeholder writer used when dynamic routing encounters a table that does not exist in the
 * catalog and auto-create is disabled. Every record written through this writer is discarded and
 * counted in a shared, factory-scoped counter so operators can distinguish "no records dropped"
 * from "records silently dropped".
 */
class NoOpWriter implements RecordWriter {
  private final AtomicLong droppedRecordCount;

  NoOpWriter(AtomicLong droppedRecordCount) {
    this.droppedRecordCount = droppedRecordCount;
  }

  @Override
  public void write(SinkRecord record) {
    droppedRecordCount.incrementAndGet();
  }

  @Override
  public List<IcebergWriterResult> complete() {
    return ImmutableList.of();
  }

  @Override
  public void close() {}
}
