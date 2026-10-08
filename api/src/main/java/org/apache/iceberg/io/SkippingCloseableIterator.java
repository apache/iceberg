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
package org.apache.iceberg.io;

/**
 * An iterator over the rows of a file that knows the position of each row in the file and can skip
 * ahead to a position.
 *
 * @param <T> the type of the rows
 */
public interface SkippingCloseableIterator<T> extends CloseableIterator<T> {
  /**
   * Returns the position in the file of the row that the next call to {@link #next()} returns.
   *
   * @return the position of the next row
   * @throws java.util.NoSuchElementException if there are no more rows
   */
  long position();

  /**
   * Skips the rows before a position, so the next row returned is the first one at or after it.
   *
   * <p>Does nothing if the next row is already at or after the position.
   *
   * @param position the position in the file to skip to
   */
  void advanceTo(long position);
}
