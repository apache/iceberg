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
package org.apache.iceberg.puffin;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public final class Blob {
  private final String type;
  private final List<Integer> inputFields;
  private final long snapshotId;
  private final long sequenceNumber;
  private final ByteBuffer blobData;
  private final PuffinCompressionCodec requestedCompression;
  private final Map<String, String> properties;

  public Blob(
      String type,
      List<Integer> inputFields,
      long snapshotId,
      long sequenceNumber,
      ByteBuffer blobData) {
    this(type, inputFields, snapshotId, sequenceNumber, blobData, null, ImmutableMap.of());
  }

  public Blob(
      String type,
      List<Integer> inputFields,
      long snapshotId,
      long sequenceNumber,
      ByteBuffer blobData,
      @Nullable PuffinCompressionCodec requestedCompression,
      Map<String, String> properties) {
    Preconditions.checkNotNull(type, "type is null");
    Preconditions.checkNotNull(inputFields, "inputFields is null");
    Preconditions.checkNotNull(blobData, "blobData is null");
    Preconditions.checkNotNull(properties, "properties is null");
    this.type = type;
    this.inputFields = ImmutableList.copyOf(inputFields);
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.blobData = blobData;
    this.requestedCompression = requestedCompression;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public String type() {
    return type;
  }

  public List<Integer> inputFields() {
    return inputFields;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public long sequenceNumber() {
    return sequenceNumber;
  }

  public ByteBuffer blobData() {
    return blobData;
  }

  @Nullable
  public PuffinCompressionCodec requestedCompression() {
    return requestedCompression;
  }

  public Map<String, String> properties() {
    return properties;
  }
}
