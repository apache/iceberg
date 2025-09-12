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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * The aggregated results of a single checkpoint which should be committed. Containing the
 * serialized {@link DeltaManifests} file - which contains the commit data, and the jobId,
 * operatorId, checkpointId triplet to identify the specific commit.
 *
 * <p>{@link DynamicCommittableSerializer} is used to serialize {@link DynamicCommittable} between
 * the {@link DynamicWriter} and the {@link DynamicWriteResultAggregator}.
 */
class DynamicCommittable implements Serializable {

  private final WriteTarget key;
  private final byte[] manifest;
  private final String jobId;
  private final String operatorId;
  private final long checkpointId;

  DynamicCommittable(
      WriteTarget key, byte[] manifest, String jobId, String operatorId, long checkpointId) {
    this.key = key;
    this.manifest = manifest;
    this.jobId = jobId;
    this.operatorId = operatorId;
    this.checkpointId = checkpointId;
  }

  WriteTarget key() {
    return key;
  }

  byte[] manifest() {
    return manifest;
  }

  String jobId() {
    return jobId;
  }

  String operatorId() {
    return operatorId;
  }

  long checkpointId() {
    return checkpointId;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DynamicCommittable that = (DynamicCommittable) o;
    return checkpointId == that.checkpointId
        && Objects.equals(key, that.key)
        && Objects.deepEquals(manifest, that.manifest)
        && Objects.equals(jobId, that.jobId)
        && Objects.equals(operatorId, that.operatorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, Arrays.hashCode(manifest), jobId, operatorId, checkpointId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("jobId", jobId)
        .add("checkpointId", checkpointId)
        .add("operatorId", operatorId)
        .toString();
  }

  public WriteTarget writeTarget() {
    return key;
  }
}
