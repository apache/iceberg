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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkExecutorCache;
import org.apache.spark.util.KnownSizeEstimation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a serializable table with a known size estimate. Spark calls its
 * SizeEstimator class when broadcasting variables and this can be an expensive operation, so
 * providing a known size estimate allows that operation to be skipped.
 *
 * <p>This class also implements AutoCloseable to avoid leaking resources upon broadcasting.
 * Broadcast variables are destroyed and cleaned up on the driver and executors once they are
 * garbage collected on the driver. The implementation ensures only resources used by copies of the
 * main table are released.
 */
public class SerializableTableWithSize extends SerializableTable
    implements KnownSizeEstimation, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SerializableTableWithSize.class);
  private static final long SIZE_ESTIMATE = 32_768L;

  private final transient Object serializationMarker;

  protected SerializableTableWithSize(Table table) {
    super(table);
    this.serializationMarker = new Object();
  }

  @Override
  public long estimatedSize() {
    return SIZE_ESTIMATE;
  }

  public static Table copyOf(Table table) {
    if (table instanceof BaseMetadataTable) {
      return new SerializableMetadataTableWithSize((BaseMetadataTable) table);
    } else {
      return new SerializableTableWithSize(table);
    }
  }

  @Override
  public void close() throws Exception {
    if (serializationMarker == null) {
      LOG.info("Releasing resources");
      io().close();
    }
    invalidateCache(name());
  }

  public static class SerializableMetadataTableWithSize extends SerializableMetadataTable
      implements KnownSizeEstimation, AutoCloseable {

    private static final Logger LOG =
        LoggerFactory.getLogger(SerializableMetadataTableWithSize.class);

    private final transient Object serializationMarker;

    protected SerializableMetadataTableWithSize(BaseMetadataTable metadataTable) {
      super(metadataTable);
      this.serializationMarker = new Object();
    }

    @Override
    public long estimatedSize() {
      return SIZE_ESTIMATE;
    }

    @Override
    public void close() throws Exception {
      if (serializationMarker == null) {
        LOG.info("Releasing resources");
        io().close();
      }
      invalidateCache(name());
    }
  }

  private static void invalidateCache(String name) {
    SparkExecutorCache cache = SparkExecutorCache.get();
    if (cache != null) {
      cache.invalidate(name);
    }
  }
}
