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
package org.apache.iceberg.flink.source.split;

import java.util.Comparator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FileSequenceNumberBasedComparator implements Comparator<IcebergSourceSplit> {
  @Override
  public int compare(IcebergSourceSplit o1, IcebergSourceSplit o2) {
    Preconditions.checkArgument(
        o1.task().files().size() == 1 && o2.task().files().size() == 1,
        "Could not compare combined task. Please use 'split-open-file-cost' to prevent combining multiple files to a split");

    Long opt1 = o1.task().files().iterator().next().file().fileSequenceNumber();
    Long opt2 = o2.task().files().iterator().next().file().fileSequenceNumber();

    Preconditions.checkNotNull(
        opt1, "V2 table is needed. Sequence number should not be null for {}", o1);
    Preconditions.checkNotNull(
        opt2, "V2 table is needed. Sequence number should not be null for {}", o2);

    if (o1.splitId().equals(o2.splitId())) {
      return 0;
    }

    int temp = Long.compare(opt1, opt2);
    if (temp != 0) {
      return temp;
    } else {
      return o1.splitId().compareTo(o2.splitId());
    }
  }
}
