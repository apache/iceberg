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

package org.apache.iceberg.flink.connector.sink;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.apache.iceberg.DataFile;

public class FlinkDataFile implements Serializable {
  private final long lowWatermark;
  private final long highWatermark;
  private final DataFile dataFile;

  public FlinkDataFile(long lowWatermark, long highWatermark, DataFile dataFile) {
    this.lowWatermark = lowWatermark;
    this.highWatermark = highWatermark;
    this.dataFile = dataFile;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("low_watermark", lowWatermark)
        .add("high_watermark", highWatermark)
        .add("data_file", dataFile)
        .toString();
  }

  /**
   * only dump essential fields like lowTimestamp, highTimestamp,and path
   * TODO: lowTimestamp or lowWaterMark?
   */
  public String toCompactDump() {
    return MoreObjects.toStringHelper(this)
        .add("low_watermark", lowWatermark)
        .add("high_watermark", highWatermark)
        .add("path", dataFile.path())
        .toString();
  }

  public DataFile getIcebergDataFile() {
    return dataFile;
  }

  public long getLowWatermark() {
    return lowWatermark;
  }

  public long getHighWatermark() {
    return highWatermark;
  }
}
