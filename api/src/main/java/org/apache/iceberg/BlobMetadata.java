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

import java.util.List;
import java.util.Map;

/** A metadata about a statistics or indices blob. */
public interface BlobMetadata {
  /** Type of the blob. Never null */
  String type();

  /** ID of the Iceberg table's snapshot the blob was computed from */
  long sourceSnapshotId();

  /** Sequence number of the Iceberg table's snapshot the blob was computed from */
  long sourceSnapshotSequenceNumber();

  /** Ordered list of fields the blob was calculated from. Never null */
  List<Integer> fields();

  /** Additional properties of the blob, specific to the blob type. Never null */
  Map<String, String> properties();
}
