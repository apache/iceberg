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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A stream source that:
 * 1) emits the elements from elementsPerCheckpoint.get(0) without allowing checkpoints.
 * 2) then waits for the checkpoint to complete.
 * 3) emits the elements from elementsPerCheckpoint.get(1) without allowing checkpoints.
 * 4) then waits for the checkpoint to complete.
 * 5) ...
 *
 * <p>Util all the list from elementsPerCheckpoint are exhausted.
 */
public final class BoundedTestSource<T> implements SourceFunction<T>, CheckpointListener {

  private final List<List<T>> elementsPerCheckpoint;
  private volatile boolean running = true;

  private final AtomicInteger numCheckpointsComplete = new AtomicInteger(0);

  /**
   * Emits all those elements in several checkpoints.
   */
  public BoundedTestSource(List<List<T>> elementsPerCheckpoint) {
    this.elementsPerCheckpoint = elementsPerCheckpoint;
  }

  /**
   * Emits all those elements in a single checkpoint.
   */
  public BoundedTestSource(T... elements) {
    this(Collections.singletonList(Arrays.asList(elements)));
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    for (int checkpoint = 0; checkpoint < elementsPerCheckpoint.size(); checkpoint++) {

      final int checkpointToAwait;
      synchronized (ctx.getCheckpointLock()) {
        checkpointToAwait = numCheckpointsComplete.get() + 2;
        for (T element : elementsPerCheckpoint.get(checkpoint)) {
          ctx.collect(element);
        }
      }

      synchronized (ctx.getCheckpointLock()) {
        while (running && numCheckpointsComplete.get() < checkpointToAwait) {
          ctx.getCheckpointLock().wait(1);
        }
      }
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    numCheckpointsComplete.incrementAndGet();
  }

  @Override
  public void cancel() {
    running = false;
  }
}
