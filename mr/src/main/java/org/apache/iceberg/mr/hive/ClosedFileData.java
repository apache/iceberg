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

import java.io.Serializable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;

/**
 * Class for storing the data file properties which are needed for an Icebreg commit.
 * <ul>
 *   <li>Partition key
 *   <li>File name
 *   <li>File format
 *   <li>File size
 *   <li>Metrics
 * </ul>
 */
final class ClosedFileData implements Serializable {
  private final PartitionKey partitionKey;
  private final String fileName;
  private final FileFormat fileFormat;
  private final Long length;
  private final Metrics metrics;

  ClosedFileData(PartitionKey partitionKey, String fileName, FileFormat fileFormat, Long length, Metrics metrics) {
    this.partitionKey = partitionKey;
    this.fileName = fileName;
    this.fileFormat = fileFormat;
    this.length = length;
    this.metrics = metrics;
  }

  PartitionKey partitionKey() {
    return partitionKey;
  }

  String fileName() {
    return fileName;
  }

  FileFormat fileFormat() {
    return fileFormat;
  }

  Long length() {
    return length;
  }

  Metrics metrics() {
    return metrics;
  }
}
