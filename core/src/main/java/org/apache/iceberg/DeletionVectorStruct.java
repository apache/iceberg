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

import java.io.Serializable;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Types;

/** Mutable {@link StructLike} implementation of {@link DeletionVector}. */
class DeletionVectorStruct extends SupportsIndexProjection implements DeletionVector, Serializable {
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          DeletionVector.LOCATION,
          DeletionVector.OFFSET,
          DeletionVector.SIZE_IN_BYTES,
          DeletionVector.CARDINALITY);

  private String location = null;
  private long offset = -1L;
  private long sizeInBytes = -1L;
  private long cardinality = -1L;

  DeletionVectorStruct(Types.StructType type) {
    super(BASE_TYPE, type);
  }

  private DeletionVectorStruct(DeletionVectorStruct toCopy) {
    super(toCopy);
    this.location = toCopy.location;
    this.offset = toCopy.offset;
    this.sizeInBytes = toCopy.sizeInBytes;
    this.cardinality = toCopy.cardinality;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public long offset() {
    return offset;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public long cardinality() {
    return cardinality;
  }

  @Override
  public DeletionVectorStruct copy() {
    return new DeletionVectorStruct(this);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return location;
      case 1:
        return offset;
      case 2:
        return sizeInBytes;
      case 3:
        return cardinality;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.location = value.toString();
        break;
      case 1:
        this.offset = (Long) value;
        break;
      case 2:
        this.sizeInBytes = (Long) value;
        break;
      case 3:
        this.cardinality = (Long) value;
        break;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("location", location)
        .add("offset", offset)
        .add("size_in_bytes", sizeInBytes)
        .add("cardinality", cardinality)
        .toString();
  }
}
