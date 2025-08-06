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

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;

/**
 * A PageReadStore implementation that wraps Comet's RowGroupReader using reflection to avoid direct
 * dependencies on Comet classes.
 */
public class CometPageReadStore implements PageReadStore {
  private final Object cometRowGroupReader;
  private static Method getPageReaderMethod;
  private static Method getRowCountMethod;
  private static Method getRowIndexOffsetMethod;
  private static Method getRowIndexesMethod;

  static {
    try {
      Class<?> rowGroupReaderClass = Class.forName("org.apache.comet.parquet.RowGroupReader");
      getPageReaderMethod = rowGroupReaderClass.getMethod("getPageReader", String[].class);
      getRowCountMethod = rowGroupReaderClass.getMethod("getRowCount");
      getRowIndexOffsetMethod = rowGroupReaderClass.getMethod("getRowIndexOffset");
      // getRowIndexes method may not exist in all versions, so make it optional
      try {
        getRowIndexesMethod = rowGroupReaderClass.getMethod("getRowIndexes");
      } catch (NoSuchMethodException e) {
        getRowIndexesMethod = null;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize CometPageReadStore reflection methods", e);
    }
  }

  public CometPageReadStore(Object cometRowGroupReader) {
    this.cometRowGroupReader = cometRowGroupReader;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    try {
      return (PageReader)
          getPageReaderMethod.invoke(cometRowGroupReader, (Object) descriptor.getPath());
    } catch (Exception e) {
      throw new RuntimeException("Failed to get page reader for column: " + descriptor, e);
    }
  }

  @Override
  public long getRowCount() {
    try {
      return (Long) getRowCountMethod.invoke(cometRowGroupReader);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get row count", e);
    }
  }

  /**
   * Gets the row index offset from the underlying Comet RowGroupReader. This method uses reflection
   * to call getRowIndexOffset() which returns Optional&lt;Long&gt;.
   */
  @Override
  public Optional<Long> getRowIndexOffset() {
    try {
      return (Optional<Long>) getRowIndexOffsetMethod.invoke(cometRowGroupReader);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get row index offset", e);
    }
  }

  /**
   * Gets the row indexes from the underlying Comet RowGroupReader if available. Falls back to the
   * default implementation if the method is not available.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    if (getRowIndexesMethod != null) {
      try {
        return (Optional<PrimitiveIterator.OfLong>) getRowIndexesMethod.invoke(cometRowGroupReader);
      } catch (Exception e) {
        // Fall back to default implementation
      }
    }
    // Use default implementation from PageReadStore interface
    return PageReadStore.super.getRowIndexes();
  }

  /** Returns the underlying Comet RowGroupReader object. */
  public Object getCometRowGroupReader() {
    return cometRowGroupReader;
  }
}
