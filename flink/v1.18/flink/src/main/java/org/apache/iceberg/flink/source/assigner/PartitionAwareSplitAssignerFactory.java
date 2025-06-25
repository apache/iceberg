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
package org.apache.iceberg.flink.source.assigner;

import java.util.Collection;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;

public class PartitionAwareSplitAssignerFactory implements SplitAssignerFactory {
  public PartitionAwareSplitAssignerFactory() {}

  /**
   * Creates a new partition-aware split assigner with no initial state.
   *
   * @return a new {@link PartitionAwareSplitAssigner} instance
   */
  @Override
  public SplitAssigner createAssigner() {
    return new PartitionAwareSplitAssigner();
  }

  /**
   * Creates a new partition-aware split assigner initialized with the given state.
   *
   * <p>This method is typically used for recovery scenarios where the assigner needs to be restored
   * from a checkpoint with existing split assignments.
   *
   * @param assignerState the collection of split states to initialize the assigner with
   * @return a new {@link PartitionAwareSplitAssigner} instance initialized with the given state
   */
  @Override
  public SplitAssigner createAssigner(Collection<IcebergSourceSplitState> assignerState) {
    return new PartitionAwareSplitAssigner(assignerState);
  }
}
