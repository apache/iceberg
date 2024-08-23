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
        predicates.stream().anyMatch(p -> p.evaluate(event, lastTimeMs, currentTimeMs));
    LOG.debug(
        "Checking event: {}, at {}, last: {} with result: {}",
        event,
        currentTimeMs,
        lastTimeMs,
        result);
    return result;
  }

  static class Builder implements Serializable {
    private Integer dataFileCount;
    private Long dataFileSizeInBytes;
    private Integer posDeleteFileCount;
    private Long posDeleteRecordCount;
    private Integer eqDeleteFileCount;
    private Long eqDeleteRecordCount;
    private Integer commitCount;
    private Duration timeout;

    public Builder dataFileCount(int newDataFileCount) {
      this.dataFileCount = newDataFileCount;
      return this;
    }

    public Builder dataFileSizeInBytes(long neDataFileSizeInBytes) {
      this.dataFileSizeInBytes = neDataFileSizeInBytes;
      return this;
    }

    public Builder posDeleteFileCount(int newPosDeleteFileCount) {
      this.posDeleteFileCount = newPosDeleteFileCount;
      return this;
    }

    public Builder posDeleteRecordCount(long newPosDeleteRecordCount) {
      this.posDeleteRecordCount = newPosDeleteRecordCount;
      return this;
    }

    public Builder eqDeleteFileCount(int newEqDeleteFileCount) {
      this.eqDeleteFileCount = newEqDeleteFileCount;
      return this;
    }

    public Builder eqDeleteRecordCount(long newEqDeleteRecordCount) {
      this.eqDeleteRecordCount = newEqDeleteRecordCount;
      return this;
    }

    public Builder commitCount(int newCommitCount) {
      this.commitCount = newCommitCount;
      return this;
    }

    Builder timeout(Duration newTimeout) {
      this.timeout = newTimeout;
      return this;
    }

    TriggerEvaluator build() {
      List<Predicate> predicates = Lists.newArrayList();
      if (dataFileCount != null) {
        predicates.add((change, unused, unused2) -> change.dataFileCount() >= dataFileCount);
      }

      if (dataFileSizeInBytes != null) {
        predicates.add(
            (change, unused, unused2) -> change.dataFileSizeInBytes() >= dataFileSizeInBytes);
      }

      if (posDeleteFileCount != null) {
        predicates.add(
            (change, unused, unused2) -> change.posDeleteFileCount() >= posDeleteFileCount);
      }

      if (posDeleteRecordCount != null) {
        predicates.add(
            (change, unused, unused2) -> change.posDeleteRecordCount() >= posDeleteRecordCount);
      }

      if (eqDeleteFileCount != null) {
        predicates.add(
            (change, unused, unused2) -> change.eqDeleteFileCount() >= eqDeleteFileCount);
      }

      if (eqDeleteRecordCount != null) {
        predicates.add(
            (change, unused, unused2) -> change.eqDeleteRecordCount() >= eqDeleteRecordCount);
      }

      if (commitCount != null) {
        predicates.add((change, unused, unused2) -> change.commitCount() >= commitCount);
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
