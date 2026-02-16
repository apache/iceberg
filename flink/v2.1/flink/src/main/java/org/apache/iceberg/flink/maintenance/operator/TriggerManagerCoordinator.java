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
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
@Internal
public class TriggerManagerCoordinator extends BaseCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerManagerCoordinator.class);

  public TriggerManagerCoordinator(String operatorName, Context context) {
    super(operatorName, context);
    LOG.info("Created TriggerManagerCoordinator: {}", operatorName);
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
          if (event instanceof LockRegisterEvent) {
            registerLock((LockRegisterEvent) event);
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

  @SuppressWarnings("FutureReturnValueIgnored")
  private void registerLock(LockRegisterEvent lockRegisterEvent) {
    LOCK_RELEASE_CONSUMERS.put(
        lockRegisterEvent.lockId(),
        lock -> {
          LOG.info(
              "Send release event for lock id {}, timestamp: {} to Operator {}",
              lock.lockId(),
              lock.timestamp(),
              operatorName());
          subtaskGateways().getSubtaskGateway(0).sendEvent(lock);
        });

    PENDING_RELEASE_EVENTS.forEach(this::handleReleaseLock);
    PENDING_RELEASE_EVENTS.clear();
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
    // We don’t need to track how many locks are currently held, because when recovering from state,
    // a `recover lock` will be issued to ensure all tasks finish running and then release all
    // locks.
    // The `TriggerManagerOperator` already keeps the `TableChange` state and related information,
    // so there’s no need to store additional state here.
    runInCoordinatorThread(
        () -> {
          resultFuture.complete(new byte[0]);
        },
        String.format(Locale.ROOT, "taking checkpoint %d", checkpointId));
  }

  @Override
  public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
    runInCoordinatorThread(
        () -> {
          LOG.info("Reset to checkpoint {}", checkpointId);
          Preconditions.checkState(coordinatorThreadFactory().isCurrentThreadCoordinatorThread());
          LOCK_RELEASE_CONSUMERS.clear();
          PENDING_RELEASE_EVENTS.clear();
        },
        String.format(Locale.ROOT, "handling checkpoint %d recovery", checkpointId));
  }
}
