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

package org.apache.iceberg.flink.source;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.iceberg.CombinedScanTask;

/**
 * TODO Implement {@link LocatableInputSplit}.
 */
public class FlinkInputSplit implements InputSplit {

  private final int splitNumber;
  private final CombinedScanTask task;

  FlinkInputSplit(int splitNumber, CombinedScanTask task) {
    this.splitNumber = splitNumber;
    this.task = task;
  }

  @Override
  public int getSplitNumber() {
    return splitNumber;
  }

  CombinedScanTask getTask() {
    return task;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlinkInputSplit that = (FlinkInputSplit) o;
    return splitNumber == that.splitNumber;
  }

  @Override
  public int hashCode() {
    return splitNumber;
  }
}
