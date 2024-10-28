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
package org.apache.iceberg.parquet;

import java.util.Iterator;
import org.apache.parquet.io.api.Binary;

public interface TripleIterator<T> extends Iterator<T> {
  /**
   * Returns the definition level from the current triple.
   *
   * <p>This method does not advance this iterator.
   *
   * @return the definition level of the current triple.
   * @throws java.util.NoSuchElementException if there are no more elements
   */
  int currentDefinitionLevel();

  /**
   * Returns the repetition level from the current triple or 0 if there are no more elements.
   *
   * <p>This method does not advance this iterator.
   *
   * @return the repetition level of the current triple, or 0 if there is no current triple.
   * @throws java.util.NoSuchElementException if there are no more elements
   */
  int currentRepetitionLevel();

  /**
   * Returns the next value as an un-boxed boolean.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed boolean
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not booleans
   */
  default boolean nextBoolean() {
    throw new UnsupportedOperationException("Not a boolean column");
  }

  /**
   * Returns the next value as an un-boxed int.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed int
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not ints
   */
  default int nextInteger() {
    throw new UnsupportedOperationException("Not an integer column");
  }

  /**
   * Returns the next value as an un-boxed long.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed long
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not longs
   */
  default long nextLong() {
    throw new UnsupportedOperationException("Not a long column");
  }

  /**
   * Returns the next value as an un-boxed float.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed float
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not floats
   */
  default float nextFloat() {
    throw new UnsupportedOperationException("Not a float column");
  }

  /**
   * Returns the next value as an un-boxed double.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed double
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not doubles
   */
  default double nextDouble() {
    throw new UnsupportedOperationException("Not a double column");
  }

  /**
   * Returns the next value as a Binary.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as a Binary
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not binary
   */
  default Binary nextBinary() {
    throw new UnsupportedOperationException("Not a binary column");
  }

  /**
   * Returns null and advances the iterator.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return null
   * @throws java.util.NoSuchElementException if there are no more elements
   */
  <N> N nextNull();
}
