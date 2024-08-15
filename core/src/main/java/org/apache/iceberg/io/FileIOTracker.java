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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.Closeable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Keeps track of the {@link FileIO} instance of the given {@link TableOperations} instance and
 * closes the {@link FileIO} when {@link FileIOTracker#close()} gets called
 */
public class FileIOTracker implements Closeable {
  private final Cache<TableOperations, FileIO> tracker;

  public FileIOTracker() {
    this.tracker =
        Caffeine.newBuilder()
            .weakKeys()
            .removalListener(
                (RemovalListener<TableOperations, FileIO>)
                    (ops, fileIO, cause) -> {
                      if (null != fileIO) {
                        fileIO.close();
                      }
                    })
            .build();
  }

  public void track(TableOperations ops) {
    Preconditions.checkArgument(null != ops, "Invalid table ops: null");
    tracker.put(ops, ops.io());
  }

  @VisibleForTesting
  Cache<TableOperations, FileIO> tracker() {
    return tracker;
  }

  @Override
  public void close() {
    tracker.invalidateAll();
    tracker.cleanUp();
  }
}
