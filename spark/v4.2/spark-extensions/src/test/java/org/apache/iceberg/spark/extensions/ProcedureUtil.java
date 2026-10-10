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
package org.apache.iceberg.spark.extensions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;

public class ProcedureUtil {

  private ProcedureUtil() {}

  static PartitionStatisticsFile writePartitionStatsFile(
      long snapshotId, String statsLocation, FileIO fileIO) {
    PositionOutputStream positionOutputStream;
    try {
      positionOutputStream = fileIO.newOutputFile(statsLocation).create();
      positionOutputStream.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .fileSizeInBytes(42L)
        .path(statsLocation)
        .build();
  }

  static String statsFileLocation(String tableLocation) {
    String statsFileName = "stats-file-" + UUID.randomUUID();
    return tableLocation.replaceFirst("file:", "") + "/metadata/" + statsFileName;
  }
}
