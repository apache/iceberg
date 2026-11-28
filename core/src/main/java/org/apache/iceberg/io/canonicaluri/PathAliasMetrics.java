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
package org.apache.iceberg.io.canonicaluri;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/** Operation counters exposed by {@link CanonicalUriFileIO}. */
public final class PathAliasMetrics implements Serializable {
  private final boolean enabled;
  private final AtomicLong translationCount = new AtomicLong();
  private final AtomicLong translationFailureCount = new AtomicLong();
  private final AtomicLong unmatchedCount = new AtomicLong();
  private final AtomicLong mappingLatencyNanos = new AtomicLong();
  private final AtomicLong readCount = new AtomicLong();
  private final AtomicLong writeCount = new AtomicLong();
  private final AtomicLong deleteCount = new AtomicLong();
  private final AtomicLong bulkDeleteCount = new AtomicLong();
  private final AtomicLong listPrefixCount = new AtomicLong();
  private final AtomicLong deletePrefixCount = new AtomicLong();
  private final AtomicLong delegateFailureCount = new AtomicLong();

  PathAliasMetrics(boolean enabled) {
    this.enabled = enabled;
  }

  void translated(boolean matched, long elapsedNanos) {
    if (enabled) {
      translationCount.incrementAndGet();
      mappingLatencyNanos.addAndGet(elapsedNanos);
      if (!matched) {
        unmatchedCount.incrementAndGet();
      }
    }
  }

  void translationFailed(long elapsedNanos) {
    if (enabled) {
      translationFailureCount.incrementAndGet();
      mappingLatencyNanos.addAndGet(elapsedNanos);
    }
  }

  void read() {
    if (enabled) {
      readCount.incrementAndGet();
    }
  }

  void write() {
    if (enabled) {
      writeCount.incrementAndGet();
    }
  }

  void delete() {
    if (enabled) {
      deleteCount.incrementAndGet();
    }
  }

  void bulkDelete() {
    if (enabled) {
      bulkDeleteCount.incrementAndGet();
    }
  }

  void listPrefix() {
    if (enabled) {
      listPrefixCount.incrementAndGet();
    }
  }

  void deletePrefix() {
    if (enabled) {
      deletePrefixCount.incrementAndGet();
    }
  }

  void delegateFailed() {
    if (enabled) {
      delegateFailureCount.incrementAndGet();
    }
  }

  /**
   * Returns the number of attempted location translations.
   *
   * @return translation count
   */
  public long translationCount() {
    return translationCount.get();
  }

  /**
   * Returns the number of location translations that failed validation or mapping.
   *
   * @return failed translation count
   */
  public long translationFailureCount() {
    return translationFailureCount.get();
  }

  /**
   * Returns the number of translation attempts that used the unmatched policy.
   *
   * @return unmatched translation count
   */
  public long unmatchedCount() {
    return unmatchedCount.get();
  }

  /**
   * Returns cumulative time spent mapping locations, in nanoseconds.
   *
   * @return cumulative mapping time in nanoseconds
   */
  public long mappingLatencyNanos() {
    return mappingLatencyNanos.get();
  }

  /**
   * Returns the number of input-file requests.
   *
   * @return input-file request count
   */
  public long readCount() {
    return readCount.get();
  }

  /**
   * Returns the number of output-file requests.
   *
   * @return output-file request count
   */
  public long writeCount() {
    return writeCount.get();
  }

  /**
   * Returns the number of single-file delete requests.
   *
   * @return single-file delete request count
   */
  public long deleteCount() {
    return deleteCount.get();
  }

  /**
   * Returns the number of bulk-delete requests.
   *
   * @return bulk-delete request count
   */
  public long bulkDeleteCount() {
    return bulkDeleteCount.get();
  }

  /**
   * Returns the number of prefix-list requests.
   *
   * @return prefix-list request count
   */
  public long listPrefixCount() {
    return listPrefixCount.get();
  }

  /**
   * Returns the number of prefix-delete requests.
   *
   * @return prefix-delete request count
   */
  public long deletePrefixCount() {
    return deletePrefixCount.get();
  }

  /**
   * Returns the number of delegate calls that failed with a runtime exception.
   *
   * @return failed delegate call count
   */
  public long delegateFailureCount() {
    return delegateFailureCount.get();
  }
}
