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

import java.util.Collection;
import org.apache.iceberg.expressions.Expression;

/**
 * Scan objects are immutable and can be shared between threads. Refinement methods, like
 * {@link #select(Collection)} and {@link #filter(Expression)}, create new TableScan instances.
 */
public interface Scan<T extends Scan> {
  /**
   * Create a new {@link TableScan} from this scan's configuration that will override the {@link Table}'s behavior based
   * on the incoming pair. Unknown properties will be ignored.
   *
   * @param property name of the table property to be overridden
   * @param value value to override with
   * @return a new scan based on this with overridden behavior
   */
  T option(String property, String value);

  /**
   * Create a new {@link TableScan} from this with the schema as its projection.
   *
   * @param schema a projection schema
   * @return a new scan based on this with the given projection
   */
  T project(Schema schema);

  /**
   * Create a new {@link TableScan} from this that, if data columns where selected
   * via {@link #select(java.util.Collection)}, controls whether the match to the schema will be done
   * with case sensitivity.
   *
   * @return a new scan based on this with case sensitivity as stated
   */
  T caseSensitive(boolean caseSensitive);

  /**
   * Create a new {@link TableScan} from this that loads the column stats with each data file.
   * <p>
   * Column stats include: value count, null value count, lower bounds, and upper bounds.
   *
   * @return a new scan based on this that loads column stats.
   */
  T includeColumnStats();

  /**
   * Create a new {@link TableScan} from this that will read the given data columns. This produces
   * an expected schema that includes all fields that are either selected or used by this scan's
   * filter expression.
   *
   * @param columns column names from the table's schema
   * @return a new scan based on this with the given projection columns
   */
  T select(Collection<String> columns);

  /**
   * Create a new {@link TableScan} from the results of this filtered by the {@link Expression}.
   *
   * @param expr a filter expression
   * @return a new scan based on this with results filtered by the expression
   */
  T filter(Expression expr);
}
