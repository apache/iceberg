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

package org.apache.iceberg.flink.source.assigner.ordered;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

class AscendingTimestampSplitComparator
    implements Comparator<IcebergSourceSplit>, Serializable {

  private final TimestampAssigner<IcebergSourceSplit> timestampAssigner;

  AscendingTimestampSplitComparator(TimestampAssigner<IcebergSourceSplit> timestampAssigner) {
    this.timestampAssigner = timestampAssigner;
  }

  @Override
  public int compare(IcebergSourceSplit a, IcebergSourceSplit b) {
    if (a.splitId().equals(b.splitId())) {
      return 0;
    }
    int temp =
        Long.compare(
            timestampAssigner.extractTimestamp(a, -1), timestampAssigner.extractTimestamp(b, -1));
    if (temp != 0) {
      return temp;
    } else {
      return a.splitId().compareTo(b.splitId());
    }
  }
}
