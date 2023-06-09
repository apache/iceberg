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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Provides implementations of {@link org.apache.iceberg.flink.source.split.SerializableComparator}
 * which could be used for ordering splits. These are used by the {@link
 * org.apache.iceberg.flink.source.assigner.OrderedSplitAssignerFactory} and the {@link
 * org.apache.iceberg.flink.source.reader.IcebergSourceReader}
 */
public class SplitComparators {
  private SplitComparators() {}

  /** Comparator which orders the splits based on the file sequence number of the data files */
  public static SerializableComparator<IcebergSourceSplit> fileSequenceNumber() {
    return (IcebergSourceSplit o1, IcebergSourceSplit o2) -> {
      Preconditions.checkArgument(
          o1.task().files().size() == 1 && o2.task().files().size() == 1,
          "Could not compare combined task. Please use 'split-open-file-cost' to prevent combining multiple files to a split");

      Long seq1 = o1.task().files().iterator().next().file().fileSequenceNumber();
      Long seq2 = o2.task().files().iterator().next().file().fileSequenceNumber();

      Preconditions.checkNotNull(
          seq1,
          "Invalid file sequence number: null. Doesn't support splits written with V1 format: %s",
          o1);
      Preconditions.checkNotNull(
          seq2,
          "IInvalid file sequence number: null. Doesn't support splits written with V1 format: %s",
          o2);

      int temp = Long.compare(seq1, seq2);
      if (temp != 0) {
        return temp;
      } else {
        return o1.splitId().compareTo(o2.splitId());
      }
    };
  }
}
