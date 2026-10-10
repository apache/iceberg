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
package org.apache.iceberg.index;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** Immutable implementation of {@link IndexSnapshot}. */
public class GenericIndexSnapshot implements IndexSnapshot {

  private final long snapshotId;
  private final long sourceTableSnapshotId;
  private final long timestampMs;
  private final String trackingFile;
  private final Map<String, String> properties;
  private final ByteBuffer keyMetadata;

  private GenericIndexSnapshot(
      long snapshotId,
      long sourceTableSnapshotId,
      long timestampMs,
      String trackingFile,
      Map<String, String> properties,
      ByteBuffer keyMetadata) {
    this.snapshotId = snapshotId;
    this.sourceTableSnapshotId = sourceTableSnapshotId;
    this.timestampMs = timestampMs;
    this.trackingFile = trackingFile;
    this.properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
    this.keyMetadata = keyMetadata;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public long sourceTableSnapshotId() {
    return sourceTableSnapshotId;
  }

  @Override
  public long timestampMs() {
    return timestampMs;
  }

  @Override
  public String trackingFile() {
    return trackingFile;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  @Nullable
  public ByteBuffer keyMetadata() {
    return keyMetadata;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private long snapshotId;
    private long sourceTableSnapshotId;
    private long timestampMs;
    private String trackingFile;
    private Map<String, String> properties;
    private ByteBuffer keyMetadata;

    private Builder() {}

    public Builder snapshotId(long id) {
      this.snapshotId = id;
      return this;
    }

    public Builder sourceTableSnapshotId(long id) {
      this.sourceTableSnapshotId = id;
      return this;
    }

    public Builder timestampMs(long ts) {
      this.timestampMs = ts;
      return this;
    }

    public Builder trackingFile(String path) {
      this.trackingFile = path;
      return this;
    }

    public Builder properties(Map<String, String> props) {
      this.properties = props;
      return this;
    }

    public Builder keyMetadata(ByteBuffer metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public GenericIndexSnapshot build() {
      Preconditions.checkArgument(trackingFile != null, "tracking-file is required");
      Preconditions.checkArgument(timestampMs > 0, "timestamp-ms is required");
      return new GenericIndexSnapshot(
          snapshotId, sourceTableSnapshotId, timestampMs, trackingFile, properties, keyMetadata);
    }
  }
}
