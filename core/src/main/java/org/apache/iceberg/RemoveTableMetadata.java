/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import com.google.common.collect.Sets;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

class RemoveTableMetadata implements ExpireTableMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveTableMetadata.class);

  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private final TableOperations ops;
  private final Set<Integer> versionsToRemove = Sets.newHashSet();
  private TableMetadata base;
  private Long expireOlderThan = null;
  private Integer expireBefore = null;
  private Consumer<String> deleteFunc = defaultDelete;

  RemoveTableMetadata(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ExpireTableMetadata expireVersion(int version) {
    LOG.info("Expiring table metadata with version: {}", version);
    versionsToRemove.add(version);
    return this;
  }

  @Override
  public ExpireTableMetadata expireBefore(int version) {
    LOG.info("Expiring table metadata versions before: {}", version);
    expireBefore = version;
    return this;
  }

  @Override
  public ExpireTableMetadata expireOlderThan(long timestampMillis) {
    LOG.info("Expiring metadata older than: {} ({})", new Date(timestampMillis), timestampMillis);
    this.expireOlderThan = timestampMillis;
    return this;
  }

  @Override
  public ExpireTableMetadata deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public List<TableMetadataFile> apply() {
    this.base = ops.refresh();
    int currentVersion = base.metadataFile().version();
    return StreamSupport.stream(ops.tableMetadataFiles().spliterator(), false)
        .filter(metadataFile -> currentVersion != metadataFile.version() &&
          (versionsToRemove.contains(metadataFile.version()) ||
            (expireBefore != null && expireBefore > metadataFile.version()) ||
            (expireOlderThan != null && expireOlderThan > metadataFile.file().lastModified()))
        )
        .collect(Collectors.toList());
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
        .run(taskOps -> {
          List<TableMetadataFile> files = apply();
          files.stream()
            .map(metadataFile -> metadataFile.file().location())
            .forEach(file -> deleteFunc.accept(file));
        });
  }
}
