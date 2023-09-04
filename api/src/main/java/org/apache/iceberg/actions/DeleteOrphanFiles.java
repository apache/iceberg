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
package org.apache.iceberg.actions;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * An action that deletes orphan metadata, data and delete files in a table.
 *
 * <p>A file is considered orphan if it is not reachable by any valid snapshot. The set of actual
 * files is built by listing the underlying storage which makes this operation expensive.
 */
public interface DeleteOrphanFiles extends Action<DeleteOrphanFiles, DeleteOrphanFiles.Result> {
  /**
   * Passes a location which should be scanned for orphan files.
   *
   * <p>If not set, the root table location will be scanned potentially removing both orphan data
   * and metadata files.
   *
   * @param location the location where to look for orphan files
   * @return this for method chaining
   */
  DeleteOrphanFiles location(String location);

  /**
   * Removes orphan files only if they are older than the given timestamp.
   *
   * <p>This is a safety measure to avoid removing files that are being added to the table. For
   * example, there may be a concurrent operation adding new files while this action searches for
   * orphan files. New files may not be referenced by the metadata yet but they are not orphan.
   *
   * <p>If not set, defaults to a timestamp 3 days ago.
   *
   * @param olderThanTimestamp a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   */
  DeleteOrphanFiles olderThan(long olderThanTimestamp);

  /**
   * Passes an alternative delete implementation that will be used for orphan files.
   *
   * <p>This method allows users to customize the delete function. For example, one may set a custom
   * delete func and collect all orphan files into a set instead of physically removing them.
   *
   * <p>If not set, defaults to using the table's {@link org.apache.iceberg.io.FileIO io}
   * implementation.
   *
   * @param deleteFunc a function that will be called to delete files
   * @return this for method chaining
   */
  DeleteOrphanFiles deleteWith(Consumer<String> deleteFunc);

  /**
   * Passes an alternative executor service that will be used for removing orphaned files. This
   * service will only be used if a custom delete function is provided by {@link
   * #deleteWith(Consumer)} or if the FileIO does not {@link SupportsBulkOperations support bulk
   * deletes}. Otherwise, parallelism should be controlled by the IO specific {@link
   * SupportsBulkOperations#deleteFiles(Iterable) deleteFiles} method.
   *
   * <p>If this method is not called and bulk deletes are not supported, orphaned manifests and data
   * files will still be deleted in the current thread.
   *
   * @param executorService the service to use
   * @return this for method chaining
   */
  DeleteOrphanFiles executeDeleteWith(ExecutorService executorService);

  /**
   * Passes a prefix mismatch mode that determines how this action should handle situations when the
   * metadata references files that match listed/provided files except for authority/scheme.
   *
   * <p>Possible values are "ERROR", "IGNORE", "DELETE". The default mismatch mode is "ERROR", which
   * means an exception is thrown whenever there is a mismatch in authority/scheme. It's the
   * recommended mismatch mode and should be changed only in some rare circumstances. If there is a
   * mismatch, use {@link #equalSchemes(Map)} and {@link #equalAuthorities(Map)} to resolve
   * conflicts by providing equivalent schemes and authorities. If it is impossible to determine
   * whether the conflicting authorities/schemes are equal, set the prefix mismatch mode to "IGNORE"
   * to skip files with mismatches. If you have manually inspected all conflicting
   * authorities/schemes, provided equivalent schemes/authorities and are absolutely confident the
   * remaining ones are different, set the prefix mismatch mode to "DELETE" to consider files with
   * mismatches as orphan. It will be impossible to recover files after deletion, so the "DELETE"
   * prefix mismatch mode must be used with extreme caution.
   *
   * @param newPrefixMismatchMode mode for handling prefix mismatches
   * @return this for method chaining
   */
  default DeleteOrphanFiles prefixMismatchMode(PrefixMismatchMode newPrefixMismatchMode) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement prefixMismatchMode");
  }

  /**
   * Passes schemes that should be considered equal.
   *
   * <p>The key may include a comma-separated list of schemes. For instance, Map("s3a,s3,s3n",
   * "s3").
   *
   * @param newEqualSchemes list of equal schemes
   * @return this for method chaining
   */
  default DeleteOrphanFiles equalSchemes(Map<String, String> newEqualSchemes) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement equalSchemes");
  }

  /**
   * Passes authorities that should be considered equal.
   *
   * <p>The key may include a comma-separate list of authorities. For instance, Map("s1name,s2name",
   * "servicename").
   *
   * @param newEqualAuthorities list of equal authorities
   * @return this for method chaining
   */
  default DeleteOrphanFiles equalAuthorities(Map<String, String> newEqualAuthorities) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement equalAuthorities");
  }

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns locations of orphan files. */
    Iterable<String> orphanFileLocations();
  }

  /**
   * Defines the action behavior when location prefixes (scheme/authority) mismatch.
   *
   * <p>{@link #ERROR} - throw an exception. {@link #IGNORE} - no action. {@link #DELETE} - delete
   * files.
   */
  enum PrefixMismatchMode {
    ERROR,
    IGNORE,
    DELETE;

    public static PrefixMismatchMode fromString(String modeAsString) {
      Preconditions.checkArgument(modeAsString != null, "Invalid mode: null");
      try {
        return PrefixMismatchMode.valueOf(modeAsString.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("Invalid mode: %s", modeAsString), e);
      }
    }
  }
}
