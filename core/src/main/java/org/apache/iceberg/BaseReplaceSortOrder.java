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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;

public class BaseReplaceSortOrder implements ReplaceSortOrder {
  private final TableOperations ops;
  private final SortOrder.Builder builder;
  private TableMetadata base;
  private final List<Validation> pendingValidations = Lists.newArrayList();

  BaseReplaceSortOrder(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.builder = SortOrder.builderFor(base.schema());
  }

  @Override
  public SortOrder apply() {
    return builder.build();
  }

  @Override
  public void validate(List<Validation> validations) {
    ValidationUtils.validate(base, validations);
    pendingValidations.addAll(validations);
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              this.base = ops.refresh();
              SortOrder newOrder = apply();
              TableMetadata updated = base.replaceSortOrder(newOrder);
              ValidationUtils.validate(base, pendingValidations);
              taskOps.commit(base, updated);
            });
  }

  @Override
  public ReplaceSortOrder asc(Term term, NullOrder nullOrder) {
    builder.asc(term, nullOrder);
    return this;
  }

  @Override
  public ReplaceSortOrder desc(Term term, NullOrder nullOrder) {
    builder.desc(term, nullOrder);
    return this;
  }

  @Override
  public ReplaceSortOrder caseSensitive(boolean caseSensitive) {
    builder.caseSensitive(caseSensitive);
    return this;
  }
}
