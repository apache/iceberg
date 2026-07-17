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
package org.apache.iceberg.flink.source.lookup;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class InMemoryLookupCache implements IcebergLookupCache {

  private final Map<RowData, List<RowData>> cache = Maps.newHashMap();

  @Override
  public @Nullable List<RowData> get(RowData key) {
    return cache.get(key);
  }

  @Override
  public void add(RowData key, RowData row) {
    cache.computeIfAbsent(key, k -> Lists.newLinkedList()).add(row);
  }

  @Override
  public void completeLoad() {
    // no-op: rows are visible as soon as they are added
  }

  @Override
  public void close() {
    cache.clear();
  }
}
