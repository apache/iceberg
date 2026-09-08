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
package org.apache.iceberg.spark;

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.primitives.Longs;

/** Represents a time-travel specification for loading tables at a specific point in time. */
public sealed interface TimeTravel permits TimeTravel.AsOfVersion, TimeTravel.AsOfTimestamp {

  static TimeTravel version(String version) {
    return new AsOfVersion(version);
  }

  static TimeTravel timestampMicros(long timestamp) {
    return new AsOfTimestamp(timestamp);
  }

  static TimeTravel timestampMillis(long timestamp) {
    return new AsOfTimestamp(TimeUnit.MILLISECONDS.toMicros(timestamp));
  }

  /** Time-travel specification using a version (snapshot ID, branch, or tag). */
  record AsOfVersion(String version) implements TimeTravel {

    public boolean isSnapshotId() {
      return Longs.tryParse(version) != null;
    }

    @Override
    public String toString() {
      return "VERSION AS OF '" + version + "'";
    }
  }

  /** Time-travel specification using a timestamp in microseconds. */
  record AsOfTimestamp(long timestampMicros) implements TimeTravel {

    public long timestampMillis() {
      return TimeUnit.MICROSECONDS.toMillis(timestampMicros);
    }

    @Override
    public String toString() {
      return "TIMESTAMP AS OF " + timestampMicros;
    }
  }
}
