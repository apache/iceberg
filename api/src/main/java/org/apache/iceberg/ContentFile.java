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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Superinterface of {@link DataFile} and {@link DeleteFile} that exposes common methods.
 *
 * @param <F> the concrete Java class of a ContentFile instance.
 */
public interface ContentFile<F> {
  /**
   * Returns the ordinal position of the file in a manifest, or null if it was not read from a
   * manifest.
   */
  Long pos();

  /** Returns id of the partition spec used for partition metadata. */
  int specId();

  /**
   * Returns type of content stored in the file; one of DATA, POSITION_DELETES, or EQUALITY_DELETES.
   */
  FileContent content();

  /** Returns fully qualified path to the file, suitable for constructing a Hadoop Path. */
  CharSequence path();

  /** Returns format of the file. */
  FileFormat format();

  /** Returns partition for this file as a {@link StructLike}. */
  StructLike partition();

  /** Returns the number of top-level records in the file. */
  long recordCount();

  /** Returns the file size in bytes. */
  long fileSizeInBytes();

  /**
   * Returns if collected, map from column ID to the size of the column in bytes, null otherwise.
   */
  Map<Integer, Long> columnSizes();

  /**
   * Returns if collected, map from column ID to the count of its values (including null and NaN
   * values), null otherwise.
   */
  Map<Integer, Long> valueCounts();

  /** Returns if collected, map from column ID to its null value count, null otherwise. */
  Map<Integer, Long> nullValueCounts();

  /** Returns if collected, map from column ID to its NaN value count, null otherwise. */
  Map<Integer, Long> nanValueCounts();

  /** Returns if collected, map from column ID to value lower bounds, null otherwise. */
  Map<Integer, ByteBuffer> lowerBounds();

  /** Returns if collected, map from column ID to value upper bounds, null otherwise. */
  Map<Integer, ByteBuffer> upperBounds();

  /**
   * Returns metadata about how this file is encrypted, or null if the file is stored in plain text.
   */
  ByteBuffer keyMetadata();

  /**
   * Returns list of recommended split locations, if applicable, null otherwise.
   *
   * <p>When available, this information is used for planning scan tasks whose boundaries are
   * determined by these offsets. The returned list must be sorted in ascending order.
   */
  List<Long> splitOffsets();

  /**
   * Returns the set of field IDs used for equality comparison, in equality delete files.
   *
   * <p>An equality delete file may contain additional data fields that are not used by equality
   * comparison. The subset of columns in a delete file to be used in equality comparison are
   * tracked by ID. Extra columns can be used to reconstruct changes and metrics from extra columns
   * are used during job planning.
   *
   * @return IDs of the fields used in equality comparison with the records in this delete file
   */
  List<Integer> equalityFieldIds();

  /**
   * Returns the sort order id of this file, which describes how the file is ordered. This
   * information will be useful for merging data and equality delete files more efficiently when
   * they share the same sort order id.
   */
  default Integer sortOrderId() {
    return null;
  }

  /**
   * Returns the data sequence number of the file.
   *
   * <p>This method represents the sequence number to which the file should apply. Note the data
   * sequence number may differ from the sequence number of the snapshot in which the underlying
   * file was added (a.k.a the file sequence number). New snapshots can add files that belong to
   * older sequence numbers (e.g. compaction). The data sequence number also does not change when
   * the file is marked as deleted.
   *
   * <p>This method can return null if the data sequence number is unknown. This may happen while
   * reading a v2 manifest that did not persist the data sequence number for manifest entries with
   * status DELETED (older Iceberg versions).
   */
  default Long dataSequenceNumber() {
    return null;
  }

  /**
   * Returns the file sequence number.
   *
   * <p>The file sequence number represents the sequence number of the snapshot in which the
   * underlying file was added. The file sequence number is always assigned at commit and cannot be
   * provided explicitly, unlike the data sequence number. The file sequence number does not change
   * upon assigning. In case of rewrite (like compaction), file sequence number can be higher than
   * the data sequence number.
   *
   * <p>This method can return null if the file sequence number is unknown. This may happen while
   * reading a v2 manifest that did not persist the file sequence number for manifest entries with
   * status EXISTING or DELETED (older Iceberg versions).
   */
  default Long fileSequenceNumber() {
    return null;
  }

  /**
   * Copies this file. Manifest readers can reuse file instances; use this method to copy data when
   * collecting files from tasks.
   *
   * @return a copy of this data file
   */
  F copy();

  /**
   * Copies this file without file stats. Manifest readers can reuse file instances; use this method
   * to copy data without stats when collecting files.
   *
   * @return a copy of this data file, without lower bounds, upper bounds, value counts, null value
   *     counts, or nan value counts
   */
  F copyWithoutStats();

  /**
   * Copies this file with column stats only for specific columns. Manifest readers can reuse file
   * instances; use this method to copy data with stats only for specific columns when collecting
   * files.
   *
   * @param requestedColumnIds column IDs for which to keep stats.
   * @return a copy of data file, with lower bounds, upper bounds, value counts, null value counts,
   *     and nan value counts for only specific columns.
   */
  default F copyWithStats(Set<Integer> requestedColumnIds) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement copyWithStats");
  }

  /**
   * Copies this file (potentially without file stats). Manifest readers can reuse file instances;
   * use this method to copy data when collecting files from tasks.
   *
   * @param withStats Will copy this file without file stats if set to <code>false</code>.
   * @return a copy of this data file. If <code>withStats</code> is set to <code>false</code> the
   *     file will not contain lower bounds, upper bounds, value counts, null value counts, or nan
   *     value counts
   */
  default F copy(boolean withStats) {
    return withStats ? copy() : copyWithoutStats();
  }
}
