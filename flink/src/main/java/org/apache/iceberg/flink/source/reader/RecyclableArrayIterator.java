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

import javax.annotation.Nullable;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

/**
 * Similar to the ArrayResultIterator.
 * Main difference is the records array can be recycled back to a pool.
 */
public final class RecyclableArrayIterator<E> implements BulkFormat.RecordIterator<E> {

  private Pool.Recycler<E[]> recycler;
  private E[] records;
  private int num;
  private int pos;

  private final MutableRecordAndPosition<E> recordAndPosition;

  public RecyclableArrayIterator(Pool.Recycler<E[]> recycler) {
    this.recycler = recycler;
    this.recordAndPosition = new MutableRecordAndPosition<>();
  }

  // -------------------------------------------------------------------------
  //  Setting
  // -------------------------------------------------------------------------

  /**
   * Sets the records to be returned by this iterator.
   * Each record's {@link RecordAndPosition} will have the same offset (for {@link RecordAndPosition#getOffset()}.
   * The first returned record will have a records-to-skip count of {@code skipCountOfFirst + 1}, following
   * the contract that each record needs to point to the position AFTER itself
   * (because a checkpoint taken after the record was emitted needs to resume from after that record).
   */
  public void set(final E[] newRecords, final int newNum, final long offset, final long skipCountOfFirst) {
    this.records = newRecords;
    this.num = newNum;
    this.pos = 0;
    this.recordAndPosition.set(null, offset, skipCountOfFirst);
  }

  // -------------------------------------------------------------------------
  //  Result Iterator Methods
  // -------------------------------------------------------------------------

  @Nullable
  @Override
  public RecordAndPosition<E> next() {
    if (pos < num) {
      recordAndPosition.setNext(records[pos++]);
      return recordAndPosition;
    } else {
      return null;
    }
  }

  @Override
  public void releaseBatch() {
    recycler.recycle(records);
  }
}
