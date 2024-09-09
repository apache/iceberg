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

import java.util.List;
import org.apache.parquet.column.page.PageReadStore;

public interface ParquetValueReader<T> {
  T read(T reuse);

  TripleIterator<?> column();

  List<TripleIterator<?>> columns();

  /**
   * @deprecated since 1.6.0, will be removed in 1.7.0; use setPageSource(PageReadStore) instead.
   */
  @Deprecated
  void setPageSource(PageReadStore pageStore, long rowPosition);

  default void setPageSource(PageReadStore pageStore) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement setPageSource(PageReadStore)");
  }
}
