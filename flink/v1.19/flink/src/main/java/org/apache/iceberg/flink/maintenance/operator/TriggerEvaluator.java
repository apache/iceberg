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

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
class TriggerEvaluator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerEvaluator.class);
  private final List<Predicate> predicates;

  private TriggerEvaluator(List<Predicate> predicates) {
    Preconditions.checkArgument(!predicates.isEmpty(), "Provide at least 1 condition.");

    this.predicates = predicates;
  }

  boolean check(TableChange event, long lastTimeMs, long currentTimeMs) {
    boolean result =
        predicates.stream()
            .anyMatch(
                p -> {
                  try {
                    return p.evaluate(event, lastTimeMs, currentTimeMs);
                  } catch (Exception e) {
                    throw new RuntimeException("Error accessing state", e);
                  }
                });
    LOG.debug(
        "Checking event: {}, at {}, last: {} with result: {}",
        event,
        currentTimeMs,
        lastTimeMs,
        result);
    return result;
  }

  static class Builder implements Serializable {
    private Integer commitNumber;
    private Integer fileNumber;
    private Long fileSize;
    private Integer deleteFileNumber;
    private Duration timeout;

    Builder commitNumber(int newCommitNumber) {
      this.commitNumber = newCommitNumber;
      return this;
    }

    Builder fileNumber(int newFileNumber) {
      this.fileNumber = newFileNumber;
      return this;
    }

    Builder fileSize(long newFileSize) {
      this.fileSize = newFileSize;
      return this;
    }

    Builder deleteFileNumber(int newDeleteFileNumber) {
      this.deleteFileNumber = newDeleteFileNumber;
      return this;
    }

    Builder timeout(Duration newTimeout) {
      this.timeout = newTimeout;
      return this;
    }

    TriggerEvaluator build() {
      List<Predicate> predicates = Lists.newArrayList();
      if (commitNumber != null) {
        predicates.add((change, unused, unused2) -> change.commitNum() >= commitNumber);
      }

      if (fileNumber != null) {
        predicates.add(
            (change, unused, unused2) ->
                change.dataFileNum() + change.deleteFileNum() >= fileNumber);
      }

      if (fileSize != null) {
        predicates.add(
            (change, unused, unused2) ->
                change.dataFileSize() + change.deleteFileSize() >= fileSize);
      }

      if (deleteFileNumber != null) {
        predicates.add((change, unused, unused2) -> change.deleteFileNum() >= deleteFileNumber);
      }

      if (timeout != null) {
        predicates.add(
            (change, lastTimeMs, currentTimeMs) ->
                currentTimeMs - lastTimeMs >= timeout.toMillis());
      }

      return new TriggerEvaluator(predicates);
    }
  }

  private interface Predicate extends Serializable {
    boolean evaluate(TableChange event, long lastTimeMs, long currentTimeMs);
  }
}
