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

package org.apache.iceberg.hive.legacy;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;


/**
 * Metadata for a data directory referenced by either a Hive table or a partition
 */
class DirectoryInfo {
  private final String location;
  private final FileFormat format;
  private final StructLike partitionData;

  DirectoryInfo(String location, FileFormat format, StructLike partitionData) {
    this.location = location;
    this.format = format;
    this.partitionData = partitionData;
  }

  public String location() {
    return location;
  }

  public FileFormat format() {
    return format;
  }

  public StructLike partitionData() {
    return partitionData;
  }
}
