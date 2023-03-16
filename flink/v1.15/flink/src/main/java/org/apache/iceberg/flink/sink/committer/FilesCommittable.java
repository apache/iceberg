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

import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class FilesCommittable implements Serializable {
  private final byte[] manifest;
  private WriteResult writeResult;
  private String jobId = "";
  private long checkpointId;
  private int subtaskId;

  public FilesCommittable(byte[] manifest) {
    this.manifest = manifest;
  }

  public FilesCommittable(byte[] manifest, String jobId, long checkpointId, int subtaskId) {
    this.manifest = manifest;
    this.jobId = jobId;
    this.checkpointId = checkpointId;
    this.subtaskId = subtaskId;
  }

  public void writeResult(WriteResult newWriteResult) {
    this.writeResult = newWriteResult;
  }

  public WriteResult writeResult() {
    return writeResult;
  }

  public byte[] manifest() {
    return manifest;
  }

  public Long checkpointId() {
    return checkpointId;
  }

  public String jobID() {
    return jobId;
  }

  public void jobID(String newJobId) {
    this.jobId = newJobId;
  }

  public void checkpointId(long newCheckpointId) {
    this.checkpointId = newCheckpointId;
  }

  public void subtaskId(int newSubtaskId) {
    this.subtaskId = newSubtaskId;
  }

  public int subtaskId() {
    return subtaskId;
  }

  /**
   * Regenerate the WriteResult from the DeltaManifests file, which uses IO to get the real file
   * information from the storage system.
   */
  public static WriteResult readFromManifest(FilesCommittable committable, FileIO io)
      throws IOException {
    byte[] manifestBytes = committable.manifest();

    DeltaManifests deltaManifests =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, manifestBytes);

    return FlinkManifestUtil.readCompletedFiles(deltaManifests, io);
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  public static byte[] writeToManifest(
      WriteResult writeResult,
      ManifestOutputFileFactory manifestOutputFileFactory,
      PartitionSpec spec)
      throws IOException {

    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(writeResult, manifestOutputFileFactory::create, spec);

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobID", jobId)
        .add("checkpointId", checkpointId)
        .add("subtaskId", subtaskId)
        .toString();
  }
}
