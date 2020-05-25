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

/**
 * Superinterface of {@link DataFile} and {@link DeleteFile} that exposes common methods.
 *
 * @param <F> the concrete Java class of a ContentFile instance.
 */
public interface ContentFile<F> {
  /**
   * @return type of content stored in the file; one of DATA, POSITION_DELETES, or EQUALITY_DELETES
   */
  FileContent content();

  /**
   * @return fully qualified path to the file, suitable for constructing a Hadoop Path
   */
  CharSequence path();

  /**
   * @return format of the file
   */
  FileFormat format();

  /**
   * @return partition for this file as a {@link StructLike}
   */
  StructLike partition();

  /**
   * @return the number of top-level records in the file
   */
  long recordCount();

  /**
   * @return the file size in bytes
   */
  long fileSizeInBytes();

  /**
   * @return if collected, map from column ID to the size of the column in bytes, null otherwise
   */
  Map<Integer, Long> columnSizes();

  /**
   * @return if collected, map from column ID to the count of its non-null values, null otherwise
   */
  Map<Integer, Long> valueCounts();

  /**
   * @return if collected, map from column ID to its null value count, null otherwise
   */
  Map<Integer, Long> nullValueCounts();

  /**
   * @return if collected, map from column ID to value lower bounds, null otherwise
   */
  Map<Integer, ByteBuffer> lowerBounds();

  /**
   * @return if collected, map from column ID to value upper bounds, null otherwise
   */
  Map<Integer, ByteBuffer> upperBounds();

  /**
   * @return metadata about how this file is encrypted, or null if the file is stored in plain
   *         text.
   */
  ByteBuffer keyMetadata();

  /**
   * @return List of recommended split locations, if applicable, null otherwise.
   * When available, this information is used for planning scan tasks whose boundaries
   * are determined by these offsets. The returned list must be sorted in ascending order.
   */
  List<Long> splitOffsets();


  /**
   * Copies this file. Manifest readers can reuse file instances; use
   * this method to copy data when collecting files from tasks.
   *
   * @return a copy of this data file
   */
  F copy();

  /**
   * Copies this file without file stats. Manifest readers can reuse file instances; use
   * this method to copy data without stats when collecting files.
   *
   * @return a copy of this data file, without lower bounds, upper bounds, value counts, or null value counts
   */
  F copyWithoutStats();
}
