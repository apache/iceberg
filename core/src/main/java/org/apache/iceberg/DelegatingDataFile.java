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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

class DelegatingDataFile implements DataFile {
  private DataFile wrapped;

  DelegatingDataFile(DataFile delegate) {
    this.wrapped = delegate;
  }

  public DataFile wrap(DataFile file) {
    this.wrapped = file;
    return this;
  }

  @Override
  public String manifestLocation() {
    return wrapped.manifestLocation();
  }

  @Override
  public Long pos() {
    return wrapped.pos();
  }

  @Override
  public int specId() {
    return wrapped.specId();
  }

  @Override
  public CharSequence path() {
    return wrapped.location();
  }

  @Override
  public FileFormat format() {
    return wrapped.format();
  }

  @Override
  public StructLike partition() {
    return wrapped.partition();
  }

  @Override
  public long recordCount() {
    return wrapped.recordCount();
  }

  @Override
  public long fileSizeInBytes() {
    return wrapped.fileSizeInBytes();
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return wrapped.columnSizes();
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return wrapped.valueCounts();
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return wrapped.nullValueCounts();
  }

  @Override
  public Map<Integer, Long> nanValueCounts() {
    return wrapped.nanValueCounts();
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return wrapped.lowerBounds();
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return wrapped.upperBounds();
  }

  @Override
  public ByteBuffer keyMetadata() {
    return wrapped.keyMetadata();
  }

  @Override
  public List<Long> splitOffsets() {
    return wrapped.splitOffsets();
  }

  @Override
  public Integer sortOrderId() {
    return wrapped.sortOrderId();
  }

  @Override
  public Long firstRowId() {
    return wrapped.firstRowId();
  }

  @Override
  public Long dataSequenceNumber() {
    return wrapped.dataSequenceNumber();
  }

  @Override
  public Long fileSequenceNumber() {
    return wrapped.fileSequenceNumber();
  }

  @Override
  public DataFile copy() {
    throw new IllegalArgumentException("Cannot copy wrapped DataFile");
  }

  @Override
  public DataFile copyWithoutStats() {
    throw new IllegalArgumentException("Cannot copy wrapped DataFile");
  }
}
