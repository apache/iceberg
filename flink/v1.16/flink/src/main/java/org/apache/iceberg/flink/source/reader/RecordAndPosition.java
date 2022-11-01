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

import org.apache.flink.annotation.Internal;

/**
 * A record along with the reader position to be stored in the checkpoint.
 *
 * <p>The position defines the point in the reader AFTER the record. Record processing and updating
 * checkpointed state happens atomically. The position points to where the reader should resume
 * after this record is processed.
 *
 * <p>This mutable object is useful in cases where only one instance of a {@code RecordAndPosition}
 * is needed at a time. Then the same instance of RecordAndPosition can be reused.
 */
@Internal
public class RecordAndPosition<T> {
  private T record;
  private int fileOffset;
  private long recordOffset;

  public RecordAndPosition(T record, int fileOffset, long recordOffset) {
    this.record = record;
    this.fileOffset = fileOffset;
    this.recordOffset = recordOffset;
  }

  public RecordAndPosition() {}

  // ------------------------------------------------------------------------

  public T record() {
    return record;
  }

  public int fileOffset() {
    return fileOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  /** Updates the record and position in this object. */
  public void set(T newRecord, int newFileOffset, long newRecordOffset) {
    this.record = newRecord;
    this.fileOffset = newFileOffset;
    this.recordOffset = newRecordOffset;
  }

  /** Sets the next record of a sequence. This increments the {@code recordOffset} by one. */
  public void record(T nextRecord) {
    this.record = nextRecord;
    this.recordOffset++;
  }

  @Override
  public String toString() {
    return String.format("%s @ %d + %d", record, fileOffset, recordOffset);
  }
}
