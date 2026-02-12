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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinator for LockRemoverOperator. Handles lock release events from downstream operators. */
@Experimental
@Internal
public class LockRemoverCoordinator extends BaseCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(LockRemoverCoordinator.class);

  public LockRemoverCoordinator(String operatorName, Context context) {
    super(operatorName, context);
    LOG.info("Created LockRemoverCoordinator: {}", operatorName);
  }

  @Override
  public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
    runInCoordinatorThread(
        () -> {
          LOG.debug(
              "Handling event from subtask {} (#{}) of {}: {}",
              subtask,
              attemptNumber,
              operatorName(),
              event);
          if (event instanceof LockReleaseEvent) {
            handleReleaseLock((LockReleaseEvent) event);
          } else {
            throw new IllegalArgumentException(
                "Invalid operator event type: " + event.getClass().getCanonicalName());
          }
        },
        String.format(
            Locale.ROOT,
            "handling operator event %s from subtask %d (#%d)",
            event.getClass(),
            subtask,
            attemptNumber));
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
      throws Exception {
    runInCoordinatorThread(
        () -> {
          resultFuture.complete(new byte[0]);
        },
        String.format(Locale.ROOT, "taking checkpoint %d", checkpointId));
  }

  @Override
  public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
      throws Exception {}
}
