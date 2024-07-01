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
package org.apache.iceberg.flink.sink.committer;

import java.io.Serializable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * The aggregated results of a single checkpoint which should be committed. Containing the
 * serialized {@link org.apache.iceberg.flink.sink.DeltaManifests} file - which contains the commit
 * data, and the jobId, operatorId, checkpointId triplet which helps identifying the specific commit
 *
 * <p>{@link IcebergCommittableSerializer} is used for serializing the objects between the Writer
 * and the Aggregator operator and between the Aggregator and the Committer as well.
 */
@Internal
public class IcebergCommittable implements Serializable {
  private final byte[] manifest;
  private final String jobId;
  private final String operatorId;
  private final long checkpointId;

  public IcebergCommittable(byte[] manifest, String jobId, String operatorId, long checkpointId) {
    this.manifest = manifest;
    this.jobId = jobId;
    this.operatorId = operatorId;
    this.checkpointId = checkpointId;
  }

  public byte[] manifest() {
    return manifest;
  }

  public String jobId() {
    return jobId;
  }

  public String operatorId() {
    return operatorId;
  }

  public Long checkpointId() {
    return checkpointId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobId", jobId)
        .add("checkpointId", checkpointId)
        .add("operatorId", operatorId)
        .toString();
  }
}
