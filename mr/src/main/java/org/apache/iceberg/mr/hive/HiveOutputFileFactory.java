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

package org.apache.iceberg.mr.hive;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;

public class HiveOutputFileFactory extends OutputFileFactory {
  private final TaskAttemptID taskAttemptID;

  public HiveOutputFileFactory(PartitionSpec spec, FileFormat fileFormat, LocationProvider locationProvider, FileIO io,
                               EncryptionManager encryptionManager, TaskAttemptID taskAttemptID) {
    super(spec, fileFormat, locationProvider, io, encryptionManager, 0, 0);
    this.taskAttemptID = taskAttemptID;
  }

  /**
   * Override the filename generation so it contains jobId, taskId, taskAttemptId. Kept the UUID and the fileCount so
   * the filenames are similar for other writers.
   * @return The generated file name
   */
  @Override
  protected String generateFilename() {
    return format().addExtension(
        String.format("%05d-%d-%d-%s-%05d", taskAttemptID.getJobID().getId(), taskAttemptID.getTaskID().getId(),
            taskAttemptID.getId(), uuid(), nextCount()));
  }
}
