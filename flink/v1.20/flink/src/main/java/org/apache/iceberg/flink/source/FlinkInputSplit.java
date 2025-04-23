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

import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class FlinkInputSplit extends LocatableInputSplit {

  private final CombinedScanTask task;

  FlinkInputSplit(int splitNumber, CombinedScanTask task, @Nullable String[] hostnames) {
    super(splitNumber, hostnames);
    this.task = task;
  }

  CombinedScanTask getTask() {
    return task;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("splitNumber", getSplitNumber())
        .add("task", task)
        .add("hosts", Arrays.toString(getHostnames()))
        .toString();
  }
}
