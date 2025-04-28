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
import java.util.Set;

class Delegates {
  private Delegates() {}

  @SuppressWarnings("unchecked")
  static <F extends ContentFile<F>> F suppressFirstRowId(F file) {
    if (file instanceof DataFile && null != file.firstRowId()) {
      return (F)
          new DelegatingDataFile((DataFile) file) {
            @Override
            public Long firstRowId() {
              return null;
            }
          };
    }

    return file;
  }

  static DeleteFile pendingDeleteFile(DeleteFile file, Long dataSequenceNumber) {
    return new PendingDeleteFile(file, dataSequenceNumber);
  }

  static class PendingDeleteFile extends DelegatingDeleteFile {
    private final Long dataSequenceNumber;

    PendingDeleteFile(DeleteFile file, Long dataSequenceNumber) {
      super(file);
      this.dataSequenceNumber = dataSequenceNumber;
    }

    @Override
    DeleteFile wrap(DeleteFile file) {
      return new PendingDeleteFile(file, dataSequenceNumber);
    }

    @Override
    public Long dataSequenceNumber() {
      return dataSequenceNumber;
    }
  }

  static class DelegatingDataFile extends DelegatingContentFile<DataFile> implements DataFile {
    private DataFile wrappedAsDataFile;

    DelegatingDataFile(DataFile wrapped) {
      super(wrapped);
      this.wrappedAsDataFile = wrapped;
    }

    DataFile wrap(DataFile file) {
      setWrapped(file);
      this.wrappedAsDataFile = file;
      return this;
    }

    @Override
    public DataFile copy() {
      return wrap(wrappedAsDataFile.copy());
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return wrap(wrappedAsDataFile.copyWithStats(requestedColumnIds));
    }

    @Override
    public DataFile copyWithoutStats() {
      return wrap(wrappedAsDataFile.copyWithoutStats());
    }

    @Override
    public DataFile copy(boolean withStats) {
      return wrap(wrappedAsDataFile.copy(withStats));
    }
  }

  static class DelegatingDeleteFile extends DelegatingContentFile<DeleteFile>
      implements DeleteFile {
    private DeleteFile wrappedAsDeleteFile;

    DelegatingDeleteFile(DeleteFile wrapped) {
      super(wrapped);
      this.wrappedAsDeleteFile = wrapped;
    }

    DeleteFile wrap(DeleteFile file) {
      setWrapped(file);
      this.wrappedAsDeleteFile = file;
      return this;
    }

    @Override
    public String referencedDataFile() {
      return wrappedAsDeleteFile.referencedDataFile();
    }

    @Override
    public Long contentOffset() {
      return wrappedAsDeleteFile.contentOffset();
    }

    @Override
    public Long contentSizeInBytes() {
      return wrappedAsDeleteFile.contentSizeInBytes();
    }

    @Override
    public DeleteFile copy() {
      return wrap(wrappedAsDeleteFile.copy());
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return wrap(wrappedAsDeleteFile.copyWithStats(requestedColumnIds));
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return wrap(wrappedAsDeleteFile.copyWithoutStats());
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return wrap(wrappedAsDeleteFile.copy(withStats));
    }
  }

  @SuppressWarnings("VisibilityModifier")
  static class DelegatingContentFile<F extends ContentFile<F>> implements ContentFile<F> {
    protected ContentFile<F> wrapped;

    DelegatingContentFile(F wrapped) {
      this.wrapped = wrapped;
    }

    protected void setWrapped(F wrapped) {
      this.wrapped = wrapped;
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
    public FileContent content() {
      return wrapped.content();
    }

    @Override
    public CharSequence path() {
      return wrapped.location();
    }

    @Override
    public String location() {
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
    public List<Integer> equalityFieldIds() {
      return wrapped.equalityFieldIds();
    }

    @Override
    public Integer sortOrderId() {
      return wrapped.sortOrderId();
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
    public Long firstRowId() {
      return wrapped.firstRowId();
    }

    @Override
    public F copy() {
      throw new IllegalArgumentException("Cannot copy wrapped DataFile");
    }

    @Override
    public F copyWithStats(Set<Integer> requestedColumnIds) {
      throw new IllegalArgumentException("Cannot copy wrapped DataFile");
    }

    @Override
    public F copyWithoutStats() {
      throw new IllegalArgumentException("Cannot copy wrapped DataFile");
    }
  }
}
