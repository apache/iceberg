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
package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Comparators;

/**
 * Wrapper class to be able to use {@link ContentFile} in Sets and Maps.
 *
 * @param <F> the concrete Java class of a ContentFile instance
 */
public class ContentFileWrapper<F> implements ContentFile<F> {

  private ContentFile<F> file;
  // lazily computed & cached hashCode
  private transient int hashCode = 0;
  // tracks if the hash has been calculated as actually being zero to avoid re-calculating the hash.
  // this follows the hashCode() implementation from java.lang.String
  private transient boolean hashIsZero = false;

  private ContentFileWrapper(ContentFile<F> file) {
    this.file = file;
  }

  public static <T> ContentFileWrapper<T> wrap(ContentFile<T> contentFile) {
    return new ContentFileWrapper<>(contentFile);
  }

  public ContentFile<F> get() {
    return file;
  }

  public ContentFileWrapper<F> set(ContentFile<F> contentFile) {
    this.file = contentFile;
    this.hashCode = 0;
    this.hashIsZero = false;
    return this;
  }

  @Override
  public Long pos() {
    return file.pos();
  }

  @Override
  public int specId() {
    return file.specId();
  }

  @Override
  public FileContent content() {
    return file.content();
  }

  @Override
  public CharSequence path() {
    return file.path();
  }

  @Override
  public String location() {
    return file.location();
  }

  @Override
  public FileFormat format() {
    return file.format();
  }

  @Override
  public StructLike partition() {
    return file.partition();
  }

  @Override
  public long recordCount() {
    return file.recordCount();
  }

  @Override
  public long fileSizeInBytes() {
    return file.fileSizeInBytes();
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return file.columnSizes();
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return file.valueCounts();
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return file.nullValueCounts();
  }

  @Override
  public Map<Integer, Long> nanValueCounts() {
    return file.nanValueCounts();
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return file.lowerBounds();
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return file.upperBounds();
  }

  @Override
  public ByteBuffer keyMetadata() {
    return file.keyMetadata();
  }

  @Override
  public List<Long> splitOffsets() {
    return file.splitOffsets();
  }

  @Override
  public List<Integer> equalityFieldIds() {
    return file.equalityFieldIds();
  }

  @Override
  public F copy() {
    return file.copy();
  }

  @Override
  public F copyWithoutStats() {
    return file.copyWithoutStats();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ContentFileWrapper)) {
      return false;
    }

    ContentFileWrapper<?> that = (ContentFileWrapper<?>) o;
    if (null == file && null == that.file) {
      return true;
    }

    if (null == file || null == that.file) {
      return false;
    }

    if (file instanceof DeleteFile && that.file instanceof DeleteFile) {
      // this needs to be updated once deletion vectors are added
      return 0 == Comparators.charSequences().compare(file.location(), that.file.location());
    }

    return 0 == Comparators.charSequences().compare(file.location(), that.file.location());
  }

  @Override
  public int hashCode() {
    int hash = hashCode;

    // don't recalculate if the hash is actually 0
    if (hash == 0 && !hashIsZero) {
      // this needs to be updated once deletion vectors are added
      hash = Objects.hashCode(null == file ? null : file.location());
      if (hash == 0) {
        hashIsZero = true;
      } else {
        this.hashCode = hash;
      }
    }

    return hash;
  }
}
