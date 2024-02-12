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

/**
 * Represents a partition statistics file that can be used to read table data more efficiently.
 *
 * <p>Statistics are informational. A reader can choose to ignore statistics information. Statistics
 * support is not required to read the table correctly.
 */
public interface PartitionStatisticsFile {
  /** ID of the Iceberg table's snapshot the partition statistics file is associated with. */
  long snapshotId();

  /** Returns fully qualified path to the file. Never null. */
  String path();

  /** Returns the size of the partition statistics file in bytes. */
  long fileSizeInBytes();
}
