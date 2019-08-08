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

import com.google.common.collect.Lists;
import java.util.Collection;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Methods to filter files in a snapshot or manifest when reading.
 *
 * @param <T> Java class returned by filter methods, also filterable
 */
public interface Filterable<T extends Filterable<T>> extends CloseableIterable<DataFile> {
  /**
   * Selects the columns of a file manifest to read.
   * <p>
   * If columns are set multiple times, the last set of columns will be read.
   * <p>
   * If the Filterable object has partition filters, they will be added to the returned partial.
   * <p>
   * For a list of column names, see the table format specification.
   *
   * @param columns String column names to load from the manifest file
   * @return a Filterable that will load only the given columns
   */
  default T select(String... columns) {
    return select(Lists.newArrayList(columns));
  }

  /**
   * Selects the columns of a file manifest to read.
   * <p>
   * If columns are set multiple times, the last set of columns will be read.
   * <p>
   * If the Filterable object has partition filters, they will be added to the returned partial.
   * <p>
   * For a list of column names, see the table format specification.
   *
   * @param columns String column names to load from the manifest file
   * @return a Filterable that will load only the given columns
   */
  T select(Collection<String> columns);

  /**
   * Set the projection from a schema.
   *
   * @param fileProjection a projection of the DataFile schema
   * @return a Filterable that will load only the given schema's columns
   */
  T project(Schema fileProjection);

  /**
   * Adds a filter expression on partition data for matching files.
   * <p>
   * If the Filterable object already has partition filters, the new filter will be added as an
   * additional requirement. The result filter expression will be the result of expr and any
   * existing filters.
   * <p>
   * If the Filterable object has columns selected, they will be added to the returned partial.
   *
   * @param expr An expression for filtering this Filterable using partition data
   * @return a Filterable that will load only rows that match expr
   */
  T filterPartitions(Expression expr);

  /**
   * Adds a filter expression on data rows for matching files.
   * <p>
   * Expressions passed to this function will be converted to partition expressions before they are
   * used to filter data files.
   * <p>
   * If the Filterable object already has partition filters, the new filter will be added as an
   * additional requirement. The result filter expression will be the result of expr and any
   * existing filters.
   * <p>
   * If the Filterable object has columns selected, they will be added to the returned partial.
   *
   * @param expr An expression for filtering this Filterable using row data
   * @return a Filterable that will load only rows that match expr
   */
  T filterRows(Expression expr);

  /**
   * Sets case sensitivity.
   *
   * @param isCaseSensitive true if expression binding and schema projection should be case sensitive
   * @return a Filterable that will use the specified case sensitivity
   */
  T caseSensitive(boolean isCaseSensitive);

  /**
   * Sets case sensitive binding and projection.
   *
   * @return a Filterable that will case sensitive binding and projection
   */
  default T caseSensitive() {
    return caseSensitive(true);
  }

  /**
   * Sets case insensitive binding and projection.
   *
   * @return a Filterable that will case insensitive binding and projection
   */
  default T caseInsensitive() {
    return caseSensitive(false);
  }
}
