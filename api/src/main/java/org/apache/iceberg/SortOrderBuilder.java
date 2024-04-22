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

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;

/** Methods for building a sort order. */
public interface SortOrderBuilder<R> {

  /**
   * Add a field to the sort by field name, ascending with nulls first.
   *
   * @param name a field name
   * @return this for method chaining
   */
  default R asc(String name) {
    return asc(Expressions.ref(name), NullOrder.NULLS_FIRST);
  }

  /**
   * Add a field to the sort by field name, ascending with the given null order.
   *
   * @param name a field name
   * @param nullOrder a null order (first or last)
   * @return this for method chaining
   */
  default R asc(String name, NullOrder nullOrder) {
    return asc(Expressions.ref(name), nullOrder);
  }

  /**
   * Add an expression term to the sort, ascending with nulls first.
   *
   * @param term an expression term
   * @return this for method chaining
   */
  default R asc(Term term) {
    return asc(term, NullOrder.NULLS_FIRST);
  }

  /**
   * Add an expression term to the sort, ascending with the given null order.
   *
   * @param term an expression term
   * @param nullOrder a null order (first or last)
   * @return this for method chaining
   */
  R asc(Term term, NullOrder nullOrder);

  /**
   * Add a field to the sort by field name, descending with nulls first.
   *
   * @param name a field name
   * @return this for method chaining
   */
  default R desc(String name) {
    return desc(Expressions.ref(name), NullOrder.NULLS_LAST);
  }

  /**
   * Add a field to the sort by field name, descending with the given null order.
   *
   * @param name a field name
   * @param nullOrder a null order (first or last)
   * @return this for method chaining
   */
  default R desc(String name, NullOrder nullOrder) {
    return desc(Expressions.ref(name), nullOrder);
  }

  /**
   * Add an expression term to the sort, descending with nulls first.
   *
   * @param term an expression term
   * @return this for method chaining
   */
  default R desc(Term term) {
    return desc(term, NullOrder.NULLS_LAST);
  }

  /**
   * Add an expression term to the sort, descending with the given null order.
   *
   * @param term an expression term
   * @param nullOrder a null order (first or last)
   * @return this for method chaining
   */
  R desc(Term term, NullOrder nullOrder);

  /**
   * Set case sensitivity of sort column name resolution.
   *
   * @param caseSensitive when true, column name resolution is case-sensitive
   * @return this for method chaining
   */
  default R caseSensitive(boolean caseSensitive) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement caseSensitive");
  };
}
