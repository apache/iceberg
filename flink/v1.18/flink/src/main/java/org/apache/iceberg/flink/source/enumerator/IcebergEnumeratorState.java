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
package org.apache.iceberg.flink.source.enumerator;

import java.io.Serializable;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;

/** Enumerator state for checkpointing */
@Internal
public class IcebergEnumeratorState implements Serializable {
  @Nullable private final IcebergEnumeratorPosition lastEnumeratedPosition;
  private final Collection<IcebergSourceSplitState> pendingSplits;
  private int[] enumerationSplitCountHistory;

  public IcebergEnumeratorState(Collection<IcebergSourceSplitState> pendingSplits) {
    this(null, pendingSplits);
  }

  public IcebergEnumeratorState(
      @Nullable IcebergEnumeratorPosition lastEnumeratedPosition,
      Collection<IcebergSourceSplitState> pendingSplits) {
    this(lastEnumeratedPosition, pendingSplits, new int[0]);
  }

  public IcebergEnumeratorState(
      @Nullable IcebergEnumeratorPosition lastEnumeratedPosition,
      Collection<IcebergSourceSplitState> pendingSplits,
      int[] enumerationSplitCountHistory) {
    this.lastEnumeratedPosition = lastEnumeratedPosition;
    this.pendingSplits = pendingSplits;
    this.enumerationSplitCountHistory = enumerationSplitCountHistory;
  }

  @Nullable
  public IcebergEnumeratorPosition lastEnumeratedPosition() {
    return lastEnumeratedPosition;
  }

  public Collection<IcebergSourceSplitState> pendingSplits() {
    return pendingSplits;
  }

  public int[] enumerationSplitCountHistory() {
    return enumerationSplitCountHistory;
  }
}
