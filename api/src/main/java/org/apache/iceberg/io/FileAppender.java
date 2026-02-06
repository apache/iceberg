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

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Metrics;

public interface FileAppender<D> extends Closeable {
  void add(D datum);

  default void addAll(Iterator<D> values) {
    while (values.hasNext()) {
      add(values.next());
    }
  }

  default void addAll(Iterable<D> values) {
    addAll(values.iterator());
  }

  /** Returns {@link Metrics} for this file. Only valid after the file is closed. */
  Metrics metrics();

  /** Returns the length of this file. */
  long length();

  /**
   * Returns a list of recommended split locations, if applicable, null otherwise.
   *
   * <p>When available, this information is used for planning scan tasks whose boundaries are
   * determined by these offsets. The returned list must be sorted in ascending order. Only valid
   * after the file is closed.
   */
  default List<Long> splitOffsets() {
    return null;
  }
}
